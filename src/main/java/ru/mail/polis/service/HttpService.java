package ru.mail.polis.service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public final class HttpService extends HttpServer implements Service {

    private final DAO dao;

    private HttpService(final HttpServerConfig config, @NotNull final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    /**
     * Create the HTTP server.
     *
     * @param port to accept HTTP connections
     * @param dao to initialize the DAO instance within the server
     * @return config
     */
    public static Service create(final int port, @NotNull final DAO dao) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return new HttpService(config, dao);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
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

    /**
     * /v0/entity.
     *
     * @param id      - id
     * @param request - request
     * @return Response
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request) {
        if (id == null || id.isEmpty()) {
            return responseWrapper(Response.BAD_REQUEST);
        }
        try {
            final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
            return processResponse(key, request);
        }
        catch (IOException e) {
            return responseWrapper(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    /**
     * Retrieve all entities from start to end.
     * If end is missing -- retrieve all entities up to the end.
     *
     * @param start of key range (required)
     * @param end   of key range (optional)
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start, @Param("end") final String end,
            @NotNull final HttpSession session) throws IOException {

    }


    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final ByteBuffer duplicate = dao.get(key).duplicate();
            final byte[] res = new byte[duplicate.remaining()];
            duplicate.get(res);
            return responseWrapper(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return responseWrapper(Response.NOT_FOUND);
        }
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

    private Response processResponse(@NotNull final ByteBuffer key, final Request request) throws IOException {
        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET: {
                return getMethodWrapper(key);
            }
            case Request.METHOD_PUT: {
                return putMethodWrapper(key, request);
            }
            case Request.METHOD_DELETE: {
                return deleteMethodWrapper(key);
            }
            default:
                return responseWrapper(Response.METHOD_NOT_ALLOWED);
        }
    }
}