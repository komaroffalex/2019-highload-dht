package ru.mail.polis.service.cluster;

import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.TimestampRecord;
import ru.mail.polis.service.utils.RequestUtils;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Coordinators {
    @NotNull
    private final ClusterNodes nodes;
    private final Map<String, HttpClient> clusterClients;
    private final RequestUtils utils;

    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());

    /**
     * Create the cluster coordinator instance.
     *
     * @param nodes to specify cluster nodes
     * @param clusterClients to specify the HttpClients of the cluster
     * @param dao to specify current DAO
     * @param proxied to specify if the request is sent by proxying
     */
    public Coordinators(@NotNull final ClusterNodes nodes, final Map<String, HttpClient> clusterClients,
                        @NotNull final DAO dao, final boolean proxied) {
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.utils = new RequestUtils(proxied, dao);
    }

    /**
     * Coordinate the delete among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst to define request
     * @param acks to specify the amount of acks needed
     * @param session to specify if the session for output
     */
    private void coordinateDelete(final String[] replicaNodes, final Request rqst,
                                     final int acks, final HttpSession session) throws IOException {
        final AtomicInteger asks = new AtomicInteger(0);
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    utils.deleteWithTimestampMethodWrapper(RequestUtils.parseKey(rqst));
                    asks.incrementAndGet();
                } else {
                    final HttpRequest request = RequestUtils.requestBase(node, rqst).DELETE().build();
                    final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).
                            sendAsync(request, BodyHandlers.ofByteArray());
                    futures.add(futureResp);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while coordinating delete: ", e);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            return;
        }
        var all = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        all = all.thenAccept((response)->{
            try {
                session.sendResponse(utils.postProcessDeleteFutures(asks, futures, acks));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while processing delete: ", e);
            }
        });
        all.exceptionally(except -> {
            try {
                session.sendResponse(utils.postProcessDeleteFutures(asks, futures, acks));
            } catch (IOException e) {
                logger.log(Level.INFO, "Exception while processing excepted delete: ", e);
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
     * @param session to specify if the session for output
     */
    private void coordinatePut(final String[] replicaNodes, final Request rqst,
                                  final int acks, final HttpSession session) throws IOException {
        final AtomicInteger asks = new AtomicInteger(0);
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    utils.putWithTimestampMethodWrapper(RequestUtils.parseKey(rqst), rqst);
                    asks.incrementAndGet();
                } else {
                    final HttpRequest request = RequestUtils.requestBase(node, rqst)
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()))
                            .build();
                    final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).
                            sendAsync(request, BodyHandlers.ofByteArray());
                    futures.add(futureResp);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while coordinating put: ", e);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            return;
        }
        var all = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        all = all.thenAccept((response) -> {
            try {
                session.sendResponse(utils.postProcessPutFutures(asks, futures, acks));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while processing put: ", e);
            }
        });
        all.exceptionally(except -> {
            try {
                session.sendResponse(utils.postProcessPutFutures(asks, futures, acks));
            } catch (IOException e) {
                logger.log(Level.INFO, "Exception while processing excepted put: ", e);
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
     * @param session to specify if the session for output
     */
    private void coordinateGet(final String[] replicaNodes, final Request rqst,
                              final int acks, final HttpSession session) throws IOException {
        final AtomicInteger asks = new AtomicInteger(0);
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        final List<TimestampRecord> responses = new ArrayList<>();
        for (final String node : replicaNodes) {
            if (node.equals(nodes.getId())) {
                final Response resp = utils.getWithTimestampMethodWrapper(RequestUtils.parseKey(rqst));
                if (resp.getBody().length == 0) {
                    responses.add(TimestampRecord.getEmpty());
                } else {
                    responses.add(TimestampRecord.fromBytes(resp.getBody()));
                }
                asks.incrementAndGet();
            } else {
                final HttpRequest request = RequestUtils.requestBase(node, rqst).GET().build();
                final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node).
                        sendAsync(request, BodyHandlers.ofByteArray());
                futures.add(futureResp);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(utils.processResponses(replicaNodes, responses));
            return;
        }
        var all = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        all = all.thenAccept((response) -> {
            try {
                session.sendResponse(utils.postProcessGetFutures(responses, asks, futures, replicaNodes, acks));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while processing get: ", e);
            }
        });
        all.exceptionally(except -> {
            try {
                session.sendResponse(utils.postProcessGetFutures(responses, asks, futures, replicaNodes, acks));
            } catch (IOException e) {
                logger.log(Level.INFO, "Exception while processing excepted get: ", e);
            }
            return null;
        });
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
                    coordinateGet(replicaClusters, request, acks, session);
                    return;
                case Request.METHOD_PUT:
                    coordinatePut(replicaClusters, request, acks, session);
                    return;
                case Request.METHOD_DELETE:
                    coordinateDelete(replicaClusters, request, acks, session);
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
