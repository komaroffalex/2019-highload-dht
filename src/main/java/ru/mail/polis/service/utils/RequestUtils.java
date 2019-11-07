package ru.mail.polis.service.utils;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.dao.TimestampRecord;
import ru.mail.polis.service.cluster.Coordinators;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.time.temporal.ChronoUnit.SECONDS;

public final class RequestUtils {
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());
    private final boolean proxied;
    @NotNull
    private final DAORocksDB dao;

    public RequestUtils(final boolean proxied, @NotNull final DAO dao){
        this.proxied = proxied;
        this.dao = (DAORocksDB) dao;
    }

    public static ByteBuffer parseKey(final Request rqst) {
        final String id = rqst.getParameter("id=");
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }

    public static HttpRequest.Builder requestBase(String node, Request rqst) {
        return HttpRequest.newBuilder()
                .uri(URI.create(node + rqst.getURI()))
                .timeout(Duration.of(5, SECONDS))
                .setHeader("PROXY_HEADER", PROXY_HEADER);
    }

    public Response postProcessDeleteFutures(AtomicInteger asks, List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                              final int acks) {
        for (final var futureTask : futures) {
            try {
                if (futureTask.get().statusCode() == 202) {
                    asks.incrementAndGet();
                }
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
            }
        }
        if (asks.get() >= acks) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Response postProcessPutFutures(AtomicInteger asks, List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                           final int acks) {
        for (final var futureTask : futures) {
            try {
                if (futureTask.get().statusCode() == 201) {
                    asks.incrementAndGet();
                }
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while putting by proxy: ", e);
            }
        }
        if (asks.get() >= acks) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Response postProcessGetFutures(List<TimestampRecord> responses, AtomicInteger asks,
                                           List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                           final String[] replicaNodes, final int acks) throws IOException {
        for (final var futureTask : futures) {
            try {
                if (futureTask.get().statusCode() == 404 && futureTask.get().body().length == 0) {
                    responses.add(TimestampRecord.getEmpty());
                } else if (futureTask.get().statusCode() != 500) {
                    responses.add(TimestampRecord.fromBytes(futureTask.get().body()));
                }
                asks.incrementAndGet();
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while getting by proxy: ", e);
            }
        }
        if (asks.get() >= acks) {
            return processResponses(replicaNodes, responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Response processResponses(final String[] replicaNodes,
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

    public void putWithTimestampMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
    }

    public void deleteWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        dao.removeRecordWithTimestamp(key);
    }

    @NotNull
    public Response getWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
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
}
