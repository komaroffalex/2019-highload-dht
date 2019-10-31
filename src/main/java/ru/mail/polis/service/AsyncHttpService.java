package ru.mail.polis.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpClient;
import one.nio.net.Socket;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.service.cluster.ClusterNodes;
import ru.mail.polis.service.cluster.Coordinators;
import ru.mail.polis.service.cluster.RF;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AsyncHttpService extends HttpServer implements Service {
    @NotNull
    private final DAORocksDB dao;
    @NotNull
    private final Executor workerThreads;

    private final ClusterNodes nodes;
    private final int clusterSize;
    private final Map<String, HttpClient> clusterClients;

    private static final Logger logger = Logger.getLogger(AsyncHttpService.class.getName());

    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private final RF defaultRF;

    /**
     * Create the HTTP Cluster server.
     *
     * @param config HTTP server configurations
     * @param dao to initialize the DAO instance within the server
     * @param nodes to represent cluster nodes
     * @param clusterClients initialized cluster clients
     */
    public AsyncHttpService(final HttpServerConfig config, @NotNull final DAO dao,
                            @NotNull final ClusterNodes nodes,
                            @NotNull final Map<String, HttpClient> clusterClients) throws IOException {
        super(config);
        this.dao = (DAORocksDB) dao;

        this.workerThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("worker").build());
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.defaultRF = new RF(nodes.getNodes().size() / 2 + 1, nodes.getNodes().size());
        this.clusterSize = nodes.getNodes().size();
    }

    /**
     * Create the HTTP server.
     *
     * @param port to accept HTTP connections
     * @param dao to initialize the DAO instance within the server
     * @return config
     */
    public static Service create(final int port, @NotNull final DAO dao,
                                 @NotNull final ClusterNodes nodes) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        config.queueTime = 10; // ms
        final Map<String, HttpClient> clusterClients = new HashMap<>();
        for (final String it : nodes.getNodes()) {
            if (!nodes.getId().equals(it) && !clusterClients.containsKey(it)) {
                clusterClients.put(it, new HttpClient(new ConnectionString(it + "?timeout=100")));
            }
        }
        return new AsyncHttpService(config, dao, nodes, clusterClients);
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
            session.sendError(Response.BAD_REQUEST, "No specified parameters");
            return;
        }
        final String id = request.getParameter("id=");
        if (id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Id is not specified");
            return;
        }
        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }

        final String replicas = request.getParameter("replicas");
        final RF rf = RF.calculateRF(replicas, session, defaultRF, clusterSize);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final boolean proxiedF = proxied;

        if (proxied || nodes.getNodes().size() > 1) {
            final Coordinators clusterCoordinator = new Coordinators(nodes, clusterClients, dao, proxiedF);
            final String[] replicaClusters = proxied ? new String[]{nodes.getId()} : nodes.replicas(rf.getFrom(), key);
            clusterCoordinator.coordinateRequest(replicaClusters, request, rf.getAck(), session);
        } else {
            executeAsyncRequest(request, key, session);
        }
    }

    private void executeAsyncRequest(final Request request, final ByteBuffer key,
                                     final HttpSession session) throws IOException {
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
                session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                return;
        }
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        switch(request.getPath()) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "Wrong path");
                break;
        }
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) throws IOException {
        workerThreads.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    logger.log(Level.SEVERE,"Exception while processing request: ", e);
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
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records =
                    dao.range(ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8)),
                            end == null ? null : ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8)));
            ((StreamStorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyAndExtractFromByteBuffer(key);
            return new Response(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
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
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
