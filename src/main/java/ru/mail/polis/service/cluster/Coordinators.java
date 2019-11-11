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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
     */
    public Coordinators(@NotNull final ClusterNodes nodes, final Map<String, HttpClient> clusterClients,
                        @NotNull final DAO dao) {
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.utils = new RequestUtils(false, dao);
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
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>(replicaNodes.length);
        for (final String node : replicaNodes) {
            if (node.equals(nodes.getId())) futures.add(utils.asyncExecuteLocalRequest(rqst));
            else {
                final HttpRequest request = RequestUtils.requestBase(node, rqst).DELETE().build();
                final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node)
                        .sendAsync(request, BodyHandlers.ofByteArray());
                futures.add(futureResp);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            return;
        }
        final AtomicInteger asks = new AtomicInteger(0);
        var all = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        all = all.thenAccept((response)->{
            try {
                session.sendResponse(utils.postProcessDeleteFutures(asks, acks, futures));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while processing delete: ", e);
            }
        });
        all.exceptionally(except -> {
            try {
                session.sendResponse(utils.postProcessDeleteFutures(asks, acks, futures));
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
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>(replicaNodes.length);
        for (final String node : replicaNodes) {
            if (node.equals(nodes.getId())) futures.add(utils.asyncExecuteLocalRequest(rqst));
            else {
                final HttpRequest request = RequestUtils.requestBase(node, rqst)
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()))
                        .build();
                final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node)
                        .sendAsync(request, BodyHandlers.ofByteArray());
                futures.add(futureResp);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            return;
        }
        final AtomicInteger asks = new AtomicInteger(0);
        var all = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        all = all.thenAccept((response) -> {
            try {
                session.sendResponse(utils.postProcessPutFutures(asks, acks, futures));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while processing put: ", e);
            }
        });
        all.exceptionally(except -> {
            try {
                session.sendResponse(utils.postProcessPutFutures(asks, acks, futures));
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
        final List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>(replicaNodes.length);
        final List<TimestampRecord> responses = new ArrayList<>();
        for (final String node : replicaNodes) {
            if (node.equals(nodes.getId())) {
                futures.add(utils.asyncExecuteLocalRequest(rqst));
            } else {
                final HttpRequest request = RequestUtils.requestBase(node, rqst).GET().build();
                final CompletableFuture<HttpResponse<byte[]>> futureResp = clusterClients.get(node)
                        .sendAsync(request, BodyHandlers.ofByteArray());
                futures.add(futureResp);
            }
        }
        if (futures.isEmpty()) {
            session.sendResponse(utils.processResponses(replicaNodes, responses));
            return;
        }
        final AtomicInteger asks = new AtomicInteger(0);
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
                if (futures.size() == 1)
                    session.sendError(Response.GATEWAY_TIMEOUT, "Exception while getting!");
                else session.sendResponse(utils.postProcessGetFutures(responses, asks, futures, replicaNodes, acks));
            } catch (IOException e) {
                logger.log(Level.INFO, "Exception while processing excepted get: ", e);
            }
            return null;
        });
    }

    /**
     * Coordinate the request among all clusters.
     *
     * @param proxied to define whether the request is proxied
     * @param request to define request
     * @param session to specify the session where to output messages
     */
    public void coordinateRequest(final boolean proxied, final Request request,
                                  final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        final String replicas = request.getParameter("replicas");
        final RF rf = RF.calculateRF(replicas, session,
                new RF(nodes.getNodes().size() / 2 + 1, nodes.getNodes().size()), nodes.getNodes().size());
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final String[] replicaClusters = proxied ? new String[]{nodes.getId()} : nodes.replicas(rf.getFrom(), key);
        this.utils.setProxied(proxied);
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    coordinateGet(replicaClusters, request, rf.getAck(), session);
                    return;
                case Request.METHOD_PUT:
                    coordinatePut(replicaClusters, request, rf.getAck(), session);
                    return;
                case Request.METHOD_DELETE:
                    coordinateDelete(replicaClusters, request, rf.getAck(), session);
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
