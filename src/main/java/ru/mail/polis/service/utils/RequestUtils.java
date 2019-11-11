package ru.mail.polis.service.utils;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAORocksDB;
import ru.mail.polis.dao.TimestampRecord;

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
    private static final Logger logger = Logger.getLogger(RequestUtils.class.getName());
    private boolean proxied;
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

    public void setProxied(boolean proxied) {
        this.proxied = proxied;
    }

    /**
     * Get the base part of the request builder.
     *
     * @param node to specify node
     * @param rqst to specify the request
     * @return base part of the request to build
     */
    public static HttpRequest.Builder requestBase(final String node, final Request rqst) {
        return HttpRequest.newBuilder()
                .uri(URI.create(node + rqst.getURI()))
                .timeout(Duration.of(5, SECONDS))
                .setHeader("PROXY_HEADER", PROXY_HEADER);
    }

    /**
     * Process the futures associated with deleting.
     *
     * @param asks to specify current number of acks
     * @param futures to specify the future requests
     * @param acks to specify the amount of acks required
     * @return response
     */
    public Response postProcessDeleteFutures(final AtomicInteger asks,
                                             final List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                             final int acks) {
        asks.set(checkCodeAndIncrement(asks, 202, futures));
        if (asks.get() >= futures.size() || asks.get() >= acks) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private int checkCodeAndIncrement(final AtomicInteger asks, final int code,
                                      final List<CompletableFuture<HttpResponse<byte[]>>> futures){
        for (final var futureTask : futures) {
            try {
                if (futureTask.get().statusCode() == code) {
                    asks.incrementAndGet();
                }
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while trying to get the future value: ", e);
            }
        }
        return asks.get();
    }

    /**
     * Execute local request asynchronously.
     *
     * @param rqst to specify the request
     * @return future result
     */
    public CompletableFuture<HttpResponse<byte[]>> asyncExecuteLocalRequest(final Request rqst) {
        return CompletableFuture.supplyAsync(() -> {
            HttpResponseClusterImpl resp;
            try {
                switch (rqst.getMethod()) {
                    case Request.METHOD_DELETE:
                        deleteWithTimestampMethodWrapper(parseKey(rqst));
                        return new HttpResponseClusterImpl().setStatusCode(202);
                    case Request.METHOD_PUT:
                        putWithTimestampMethodWrapper(parseKey(rqst), rqst);
                        return new HttpResponseClusterImpl().setStatusCode(201);
                    case Request.METHOD_GET:
                        final Response respGet = getWithTimestampMethodWrapper(parseKey(rqst));
                        return new HttpResponseClusterImpl().setStatusCode(respGet.getStatus()).
                                setBody(respGet.getBody());
                    default:
                        resp = new HttpResponseClusterImpl().setStatusCode(405);
                }
            } catch (IOException e) {
                resp = new HttpResponseClusterImpl().setStatusCode(404);
            }
            return resp;
        });
    }

    /**
     * Process the futures associated with putting.
     *
     * @param asks to specify current number of acks
     * @param futures to specify the future requests
     * @param acks to specify the amount of acks required
     * @return response
     */
    public Response postProcessPutFutures(final AtomicInteger asks,
                                          final List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                          final int acks) {
        asks.set(checkCodeAndIncrement(asks, 201, futures));
        if (asks.get() >= futures.size() || asks.get() >= acks) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * Process the futures associated with getting.
     *
     * @param responses current list of records to fill
     * @param asks to specify current number of acks
     * @param futures to specify the future requests
     * @param replicaNodes to specify the nodes to which to replicate
     * @param acks to specify the amount of acks required
     * @return response
     */
    public Response postProcessGetFutures(final List<TimestampRecord> responses, final AtomicInteger asks,
                                          final List<CompletableFuture<HttpResponse<byte[]>>> futures,
                                          final String[] replicaNodes, final int acks) throws IOException {
        for (final var futureTask : futures) {
            try {
                if (futureTask.get().body().length == 0) {
                    responses.add(TimestampRecord.getEmpty());
                } else if (futureTask.get().statusCode() != 500) {
                    responses.add(TimestampRecord.fromBytes(futureTask.get().body()));
                }
                asks.incrementAndGet();
            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while processing get request: ", e);
            }
        }
        if (asks.get() >= futures.size() || asks.get() >= acks) {
            return processResponses(replicaNodes, responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * Process the futures associated with putting.
     *
     * @param replicaNodes to specify the nodes to which to replicate
     * @param responses list of current responses
     * @return response
     */
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

    /**
     * Access dao and put the required key.
     *
     * @param key to specify the key to put
     * @param request to specify the request
     */
    public void putWithTimestampMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
    }

    /**
     * Access dao and delete the required key.
     *
     * @param key to specify the key to put
     */
    public void deleteWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        dao.removeRecordWithTimestamp(key);
    }

    /**
     * Access dao and get the required key.
     *
     * @param key to specify the key to put
     * @return response containing the key
     */
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
