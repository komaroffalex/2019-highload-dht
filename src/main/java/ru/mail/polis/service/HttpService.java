package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class HttpService extends HttpServer implements Service {

    private DAO dao;

    private HttpService(final HttpServerConfig config, @NotNull final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    public static Service create(final int port, @NotNull final DAO dao) throws IOException {
        var acceptor = new AcceptorConfig();
        var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return new HttpService(config, dao);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") String id, Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "Id is required".getBytes(StandardCharsets.UTF_8));
        }
        try {
            var method = request.getMethod();
            var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
            switch (method) {
                case Request.METHOD_GET: {
                    var value = dao.get(key);
                    var duplicate = value.duplicate();
                    byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                }
                case Request.METHOD_PUT: {
                    var value = ByteBuffer.wrap(request.getBody());
                    dao.upsert(key, value);
                    return new Response(Response.CREATED, Response.EMPTY);
                }
                case Request.METHOD_DELETE: {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                }
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        }
        catch (IOException e) {
            return  new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
    }
}
