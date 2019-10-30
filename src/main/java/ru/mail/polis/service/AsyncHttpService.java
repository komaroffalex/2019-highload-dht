package ru.mail.polis.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.base.Splitter;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.pool.PoolException;
import one.nio.net.Socket;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.dao.TimestampRecord;
import ru.mail.polis.service.cluster.ClusterNodes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static one.nio.http.Response.*;

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
     * Create the Async HTTP server.
     *
     * @param port to accept HTTP connections
     * @param dao to initialize the DAO instance within the server
     */
    public AsyncHttpService(final int port, @NotNull final DAO dao) throws IOException {
        super(from(port));
        this.dao = (DAORocksDB) dao;
        this.workerThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("worker").build());
        this.nodes = null;
        this.clusterClients = null;
        this.defaultRF = null;
        this.clusterSize = 0;
    }

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
            session.sendError(BAD_REQUEST, "No specified parameters");
            return;
        }
        final String id = request.getParameter("id=");
        if (id.isEmpty()) {
            session.sendError(BAD_REQUEST, "Id is not specified");
            return;
        }

        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }

        final String replicas = request.getParameter("replicas");

        final RF rf;
        try {
            rf = replicas == null ? defaultRF : RF.of(replicas);
            if (rf.ack < 1 || rf.from < rf.ack || rf.from > clusterSize) {
                throw new IllegalArgumentException("From is too big!");
            }
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "Wrong RF!");
            return;
        }
        final int ask = rf.ack;
        final int from = rf.from;

        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));

        final boolean proxiedF = proxied;
        if(proxied || (nodes.getNodes().size() > 1)) {
            try {
                String[] replicaClusters = proxied ? new String[]{nodes.getId()} : nodes.replicas(from, key);
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        session.sendResponse(coordinateGet(key, replicaClusters, request, ask, proxiedF, id));
                        return;
                    case Request.METHOD_PUT:
                        session.sendResponse(coordinatePut(key, replicaClusters, request, ask, proxiedF, id, request.getBody()));
                        return;
                    case Request.METHOD_DELETE:
                        session.sendResponse(coordinateDelete(key, replicaClusters, request, ask, proxiedF, id));
                        return;
                    default:
                        session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
                        return;
                }
            } catch (IOException e) {
                session.sendError(GATEWAY_TIMEOUT, e.getMessage());
            }
        } else {
            final String keyClusterPartition = nodes.keyBelongsTo(key);
            if (!nodes.getId().equals(keyClusterPartition)) {
                executeAsync(session, () -> forwardRequestTo(keyClusterPartition, request));
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
            } catch (IOException e) {
                session.sendError(INTERNAL_ERROR, e.getMessage());
            }
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
                session.sendError(BAD_REQUEST, "Wrong path");
                break;
        }
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) throws IOException {
        workerThreads.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(INTERNAL_ERROR, e.getMessage());
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
            session.sendError(BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
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
    private Response getWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyAndExtractWithTimestampFromByteBuffer(key);
            return responseWrapper(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return responseWrapper(Response.NOT_FOUND);
        }
    }

    private byte[] copyAndExtractWithTimestampFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final TimestampRecord res = dao.getRecordWithTimestamp(key);
        if(res.isEmpty()){
            throw new NoSuchElementException("Element not found!");
        }
        return res.toBytes();
    }

    private TimestampRecord merge(final ArrayList<TimestampRecord> responses) {
        if (responses.size() == 1) return responses.get(0);
        else {
            return responses.stream()
                    .filter(timestampRecord -> !timestampRecord.isEmpty())
                    .max(Comparator.comparingLong(TimestampRecord::getTimestamp))
                    .orElseGet(TimestampRecord::getEmpty);
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

    private void putWithTimestampMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
    }

    private void deleteWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        dao.removeRecordWithTimestamp(key);
    }

    private Response responseWrapper(@NotNull final String key) {
        return new Response(key, Response.EMPTY);
    }

    private Response responseWrapper(@NotNull final String key, @NotNull final byte[] body) {
        return new Response(key, body);
    }

    private Response forwardRequestTo(@NotNull final String cluster, final Request request) throws IOException {
        try {
            return clusterClients.get(cluster).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            logger.log(Level.SEVERE,"Exception while forwarding request: ", e);
            return responseWrapper(INTERNAL_ERROR);
        }
    }

    private static final class RF {
        private final int ack;
        private final int from;

        private RF(final int ack, final int from) {
            this.ack = ack;
            this.from = from;
        }

        @NotNull
        private static RF of(@NotNull final String value) {
            String rem = value.replace("=","");
            final List<String> values = Splitter.on('/').splitToList(rem);
            if (values.size() != 2) {
                throw new IllegalArgumentException("Wrong replica factor:" + value);
            }
            return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
        }
    }

    private Response coordinateDelete(final ByteBuffer key, final String[] replicaNodes,
                                      Request rqst, final int acks, boolean proxied, String id) throws IOException {
        final String ENTITY_PATH_ID = "/v0/entity?id=";
        int asks = 0;
        for (String node : replicaNodes) {
            if (node.equals(nodes.getId())) {
                try {
                    deleteWithTimestampMethodWrapper(key);
                    asks++;
                } catch (IOException e) {
                    logger.log(Level.SEVERE,"Exception while deleting: ", e);
                }
            } else {
                try {
                    rqst.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .delete(ENTITY_PATH_ID + id, PROXY_HEADER);
                    if (resp.getStatus() == 202) {
                        asks++;
                    }
                } catch (HttpException | InterruptedException | PoolException e) {
                    logger.log(Level.SEVERE,"Exception while deleting by proxy: ", e);
                }
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response coordinatePut(final ByteBuffer key, final String[] replicaNodes,
                                      final Request rqst, final int acks, boolean proxied, String id, byte[] value) throws IOException {
        final String ENTITY_PATH_ID = "/v0/entity?id=";
        int asks = 0;
        for (String node : replicaNodes) {
            if (node.equals(nodes.getId())) {
                try {
                    putWithTimestampMethodWrapper(key, rqst);
                    asks++;
                } catch (IOException e) {
                    logger.log(Level.SEVERE,"Exception while putting: ", e);
                }
            } else {
                try {
                    rqst.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .put(ENTITY_PATH_ID + id, value, PROXY_HEADER);
                    if (resp.getStatus() == 201) {
                        asks++;
                    }
                } catch (HttpException | PoolException | InterruptedException e) {
                    logger.log(Level.SEVERE,"Exception while putting by proxy: ", e);
                }
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response coordinateGet(final ByteBuffer key, final String[] replicaNodes,
                                   final Request rqst, final int acks, boolean proxied, String id) throws IOException {
        final String ENTITY_PATH_ID = "/v0/entity?id=";
        int asks = 0;
        final ArrayList<TimestampRecord> responses = new ArrayList<>();
        for (String node : replicaNodes) {
            try {
                Response respGet;
                if (node.equals(nodes.getId())) {
                    respGet = getWithTimestampMethodWrapper(key);

                } else {
                    rqst.addHeader(PROXY_HEADER);
                    respGet = clusterClients.get(node)
                            .get(ENTITY_PATH_ID + id, PROXY_HEADER);
                }
                if (respGet.getStatus() == 404 && respGet.getBody().length == 0) {
                    responses.add(TimestampRecord.getEmpty());
                } else if (respGet.getStatus() == 500) {
                    continue;
                } else {
                    responses.add(TimestampRecord.fromBytes(respGet.getBody()));
                }
                asks++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while putting by proxy: ", e);
            }
        }
        if (asks >= acks || proxied) {
            TimestampRecord mergedResp = merge(responses);
            if(mergedResp.isValue()) {
                if(!proxied && replicaNodes.length == 1) {
                    final var val = mergedResp.getValue().duplicate();
                    final byte[] ret = new byte[val.remaining()];
                    val.get(ret);
                    return new Response(Response.OK, ret);
                } else if (proxied && replicaNodes.length == 1) {
                    return new Response(Response.OK, mergedResp.toBytes());
                } else {
                    final var val = mergedResp.getValue().duplicate();
                    final byte[] ret = new byte[val.remaining()];
                    val.get(ret);
                    return new Response(Response.OK, ret);
                }
            } else if (mergedResp.isDeleted()) {
                return new Response(Response.NOT_FOUND, mergedResp.toBytes());
            } else {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }
}
