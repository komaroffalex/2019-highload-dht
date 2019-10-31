package ru.mail.polis.service.cluster;

import one.nio.http.*;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.dao.TimestampRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static one.nio.http.Response.GATEWAY_TIMEOUT;
import static one.nio.http.Response.METHOD_NOT_ALLOWED;

public class Coordinators {
    @NotNull
    private final DAORocksDB dao;
    private final ClusterNodes nodes;
    private final Map<String, HttpClient> clusterClients;
    private boolean proxied;

    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    public Coordinators(final ClusterNodes nodes, final Map<String, HttpClient> clusterClients,
                        @NotNull final DAO dao, final boolean proxied) {
        this.dao = (DAORocksDB) dao;
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.proxied = proxied;
    }

    public Response coordinateDelete(final String[] replicaNodes, Request rqst,
                                     final int acks, boolean proxied) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        for (String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    deleteWithTimestampMethodWrapper(key);
                    asks++;
                } else {

                    rqst.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .delete(ENTITY_HEADER + id, PROXY_HEADER);
                    if (resp.getStatus() == 202) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | InterruptedException | PoolException e) {
                logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Response coordinatePut(final String[] replicaNodes, final Request rqst,
                                  final int acks, boolean proxied) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        for (String node : replicaNodes) {
            try {
                if (node.equals(nodes.getId())) {
                    putWithTimestampMethodWrapper(key, rqst);
                    asks++;
                } else {
                    rqst.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .put(ENTITY_HEADER + id, rqst.getBody(), PROXY_HEADER);
                    if (resp.getStatus() == 201) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while putting!", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Response coordinateGet(final String[] replicaNodes, final Request rqst,
                                  final int acks, boolean proxied) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
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
                            .get(ENTITY_HEADER + id, PROXY_HEADER);
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
                logger.log(Level.SEVERE, "Exception while putting!", e);
            }
        }
        if (asks >= acks || proxied) {
            return processResponses(replicaNodes, responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response processResponses(final String[] replicaNodes,
                                      final ArrayList<TimestampRecord> responses) throws IOException {
        TimestampRecord mergedResp = TimestampRecord.merge(responses);
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

    public void coordinateRequest(final String[] replicaClusters, final Request request,
                                  final int acks, HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(coordinateGet(replicaClusters, request, acks, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(coordinatePut(replicaClusters, request, acks, proxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(coordinateDelete(replicaClusters, request, acks, proxied));
                    return;
                default:
                    session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
                    return;
            }
        } catch (IOException e) {
            session.sendError(GATEWAY_TIMEOUT, e.getMessage());
        }
    }
}
