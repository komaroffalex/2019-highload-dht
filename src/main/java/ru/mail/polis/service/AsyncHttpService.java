package ru.mail.polis.service;


import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static one.nio.http.Response.METHOD_NOT_ALLOWED;
import static one.nio.http.Response.INTERNAL_ERROR;
import static one.nio.http.Response.BAD_REQUEST;

public class AsyncHttpService extends HttpServer implements Service {
    @NotNull
    private final DAO dao;
    @NotNull
    private final Executor workers;

    /**
     * Create the Async HTTP server.
     *
     * @param port to accept HTTP connections
     * @param dao to initialize the DAO instance within the server
     * @param workers thread workers
     */
    public AsyncHttpService(final int port, @NotNull final DAO dao,
                            @NotNull final Executor workers) throws IOException {
        super(from(port));
        this.dao = dao;
        this.workers = workers;
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StreamStorageSession(socket, this);
    }

    /**
     * Serve requests for status.
     *
     * @return status
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    private void entity(@NotNull final Request request, final HttpSession session) throws IOException {
        if (request.getURI().equals("/v0/entity")) {
            executeAsync(session, () -> responseWrapper(Response.BAD_REQUEST));
            return;
        }
        final String id = request.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        if (id.isEmpty()) {
            executeAsync(session, () -> responseWrapper(Response.BAD_REQUEST));
            return;
        }
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> getMethodWrapper(key));
                    return;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> putMethodWrapper(key, request));
                    return;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> deleteMethodWrapper(key));
                    return;
                default:
                    session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
                    return;
            }
        } catch (Exception e) {
            session.sendError(INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        switch(request.getPath()) {
            case "/v0/entity":
                entity(request,session);
                break;
            case "/v0/entities":
                entities(request,session);
                break;
            default:
                session.sendError(BAD_REQUEST, "Wrong path");
                break;
        }
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        workers.execute(() -> {
            try {
                session.sendResponse(action.act());
            }
            catch (Exception e) {
                try {
                    session.sendError(INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    private void entities(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");
        if (start == null || start.isEmpty()) {
            session.sendError(BAD_REQUEST, "No start");
            return;
        }

        if(request.getMethod() != Request.METHOD_GET) {
            session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        String end = request.getParameter("end=");
        if(end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records =
                    dao.range(ByteBuffer.wrap(start.getBytes()),
                            end == null ? null : ByteBuffer.wrap(end.getBytes()));
            ((StreamStorageSession) session).stream(records);
        } catch (IOException e)
        {
            session.sendError(INTERNAL_ERROR, e.getMessage());
        }
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyAndExtractFromByteBuffer(key);
            return responseWrapper(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return responseWrapper(Response.NOT_FOUND);
        }
    }

    private byte[] copyAndExtractFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final ByteBuffer dct = dao.get(key).duplicate();
        final byte[] res = new byte[dct.remaining()];
        dct.get(res);
        return res;
    }

    @NotNull
    private Response putMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return responseWrapper(Response.CREATED);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return responseWrapper(Response.ACCEPTED);
    }

    private Response responseWrapper(@NotNull final String key) {
        return new Response(key, Response.EMPTY);
    }

    private Response responseWrapper(@NotNull final String key, @NotNull final byte[] body) {
        return new Response(key, body);
    }
}
