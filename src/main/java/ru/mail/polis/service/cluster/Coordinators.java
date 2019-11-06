package ru.mail.polis.service.cluster;

//import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.dao.TimestampRecord;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.InterruptedException;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Coordinators {
    @NotNull
    private final DAORocksDB dao;
    private final ClusterNodes nodes;
    private final Map<String, HttpClient> clusterClients;
    private final boolean proxied;

    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    /**
     * Create the cluster coordinator instance.
     *
     * @param nodes to specify cluster nodes
     * @param clusterClients to specify the HttpClients of the cluster
     * @param dao to specify current DAO
     * @param proxied to specify if the request is sent by proxying
     */
    public Coordinators(final ClusterNodes nodes, final Map<String, HttpClient> clusterClients,
                        @NotNull final DAO dao, final boolean proxied) {
        this.dao = (DAORocksDB) dao;
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.proxied = proxied;
    }

    /**
     * Coordinate the delete among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst to define request
     * @param acks to specify the amount of acks needed
     * @param proxied to specify if the request is sent by proxying
     * @return Response value
     */
    public void coordinateDelete(final String[] replicaNodes, final Request rqst,
                                     final int acks, final boolean proxied,
                                     final HttpSession session) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        AtomicInteger asks = new AtomicInteger(0);
        List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    deleteWithTimestampMethodWrapper(key);
                    asks.incrementAndGet();
                } else {
                    HttpRequest request = HttpRequest.newBuilder()
                            .DELETE()
                            .uri(URI.create(node + rqst.getURI()))
                            .timeout(Duration.of(10, SECONDS))
                            .setHeader("PROXY_HEADER", PROXY_HEADER)
                            .build();
                    CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).sendAsync(request, BodyHandlers.ofByteArray());
                    futureResp.exceptionally(except->{
                        logger.log(Level.SEVERE, "Exception while deleting by proxy: ", except);
                        return null;
                    });
                    futures.add(futureResp);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
            }
        }
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        if (futures.size()==0) {
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            return;
        }
        all.thenAccept((response)->{
            try {
                for(var futureTask : futures){
                    if (futureTask.get().statusCode() == 202) {
                        asks.incrementAndGet();
                    }
                }
                if (asks.get() >= acks || proxied) {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                }
            } catch (IOException | ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
            }
        });
        all.exceptionally(except -> {
            logger.log(Level.SEVERE, "Exception while deleting by proxy: ", except);
            for(var futureTask : futures){
                try {
                    if (futureTask.get().statusCode() == 202) {
                        asks.incrementAndGet();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                }
            }
            try {
                if (asks.get() >= acks) session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                else session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
            }
            return null;
        });
    }


    /**
     * Coordinate the put among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst to define request
     * @param acks to specify the amount of acks needed
     * @param proxied to specify if the request is sent by proxying
     * @return Response value
     */
    public void coordinatePut(final String[] replicaNodes, final Request rqst,
                                  final int acks, final boolean proxied,
                                  final HttpSession session) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        AtomicInteger asks = new AtomicInteger(0);
        List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    putWithTimestampMethodWrapper(key, rqst);
                    asks.incrementAndGet();
                } else {
                    HttpRequest request = HttpRequest.newBuilder()
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()))
                            .uri(URI.create(node + rqst.getURI()))
                            .timeout(Duration.of(10, SECONDS))
                            .setHeader("PROXY_HEADER", PROXY_HEADER)
                            .build();
                    CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).sendAsync(request, BodyHandlers.ofByteArray());
                    futureResp.exceptionally(except->{
                        logger.log(Level.SEVERE, "Exception while putting by proxy: ", except);
                        return null;
                    });
                    futures.add(futureResp);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while putting!", e);
            }
        }
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        if (futures.size()==0) {
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            return;
        }
        all.thenAccept((response)->{
            try {
                for(var futureTask : futures){
                    if (futureTask.get().statusCode() == 201) {
                        asks.incrementAndGet();
                    }
                }
                if (asks.get() >= acks || proxied) {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                }
            } catch (IOException | ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while putting by proxy: ", e);
            }
        });
        all.exceptionally(except -> {
            logger.log(Level.SEVERE, "Exception while putting by proxy: ", except);
            for(var futureTask : futures){
                try {
                    if (futureTask.get().statusCode() == 201) {
                        asks.incrementAndGet();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    logger.log(Level.SEVERE, "Exception while putting by proxy: ", e);
                }
            }
            try {
                if (asks.get() >= acks) session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                else session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while putting by proxy: ", e);
            }
            return null;
        });
    }

    /**
     * Coordinate the get among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst to define request
     * @param acks to specify the amount of acks needed
     * @param proxied to specify if the request is sent by proxying
     * @return Response value
     */
    public void coordinateGet(final String[] replicaNodes, final Request rqst,
                              final int acks, final boolean proxied,
                              final HttpSession session) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        AtomicInteger asks = new AtomicInteger(0);
        List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        final List<TimestampRecord> responses = new ArrayList<>();
        for (final String node : replicaNodes) {
            if (node.equals(nodes.getId())) {
                Response resp = getWithTimestampMethodWrapper(key);
                if (resp.getBody().length != 0) {
                    responses.add(TimestampRecord.fromBytes(resp.getBody()));
                } else {
                    responses.add(TimestampRecord.getEmpty());
                }
                asks.incrementAndGet();
            } else {
                HttpRequest request = HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(node + rqst.getURI()))
                        .timeout(Duration.of(5, SECONDS))
                        .setHeader("PROXY_HEADER", PROXY_HEADER)
                        .build();
                CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).sendAsync(request, BodyHandlers.ofByteArray());
                futureResp.exceptionally(except -> {
                    logger.log(Level.SEVERE, "Exception while getting by proxy: ", except);
                    return null;
                });
                futures.add(futureResp);
            }
        }
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        if (futures.size()==0) {
            session.sendResponse(processResponses(replicaNodes, responses));
            return;
        }
        all.thenAccept((response)->{
            try {
                for (var futureTask : futures){
                    if (futureTask.get().statusCode() == 404 && futureTask.get().body().length == 0) {
                        responses.add(TimestampRecord.getEmpty());
                    } else if (futureTask.get().statusCode() != 500) {
                        responses.add(TimestampRecord.fromBytes(futureTask.get().body()));
                    }
                    asks.incrementAndGet();
                }
                if (asks.get() >= acks || proxied) {
                    session.sendResponse(processResponses(replicaNodes, responses));
                } else {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                }
            } catch (IOException | ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while getting by proxy: ", e);
            }
        });
        all.exceptionally(except -> {
            logger.log(Level.SEVERE, "Exception while getting by proxy: ", except);
            try {
                for (var futureTask : futures) {
                    if (futureTask.get().statusCode() == 404 && futureTask.get().body().length == 0) {
                        responses.add(TimestampRecord.getEmpty());
                    } else if (futureTask.get().statusCode() != 500) {
                        responses.add(TimestampRecord.fromBytes(futureTask.get().body()));
                    }
                    asks.incrementAndGet();
                }
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while getting by proxy: ", e);
            }
            try {
                if (asks.get() >= acks) session.sendResponse(processResponses(replicaNodes, responses));
                else session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while getting by proxy: ", e);
            }
            return null;
        });
    }

    private Response processResponses(final String[] replicaNodes,
                                      final List<TimestampRecord> responses) throws IOException {
        final TimestampRecord mergedResp = TimestampRecord.merge(responses);
        if(mergedResp.isValue()) {
            if(!proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResp.getValueAsBytes());
            } else if (proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResp.toBytes());
            } else {
                return new Response(Response.OK, mergedResp.getValueAsBytes());
            }
        } else if (mergedResp.isDeleted()) {
            return new Response(Response.NOT_FOUND, mergedResp.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private void putWithTimestampMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
    }

    private void deleteWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        dao.removeRecordWithTimestamp(key);
    }

    @NotNull
    private Response getWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyAndExtractWithTimestampFromByteBuffer(key);
            return new Response(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private byte[] copyAndExtractWithTimestampFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final TimestampRecord res = dao.getRecordWithTimestamp(key);
        if(res.isEmpty()){
            throw new NoSuchElementException("Element not found!");
        }
        return res.toBytes();
    }

    /**
     * Coordinate the request among all clusters.
     *
     * @param replicaClusters to define the nodes where to create replicas
     * @param request to define request
     * @param acks to specify the amount of acks needed
     * @param session to specify the session where to output messages
     */
    public void coordinateRequest(final String[] replicaClusters, final Request request,
                                  final int acks, final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    coordinateGet(replicaClusters, request, acks, proxied, session);
                    return;
                case Request.METHOD_PUT:
                    coordinatePut(replicaClusters, request, acks, proxied, session);
                    return;
                case Request.METHOD_DELETE:
                    coordinateDelete(replicaClusters, request, acks, proxied, session);
                    return;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                    return;
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }
}
