package ru.mail.polis.service.utils;

import javax.net.ssl.SSLSession;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

public class HttpResponseClusterImpl implements HttpResponse<byte[]> {
    private int statusCode;
    private byte[] body;

    public HttpResponseClusterImpl setStatusCode(final int statusCode){
        this.statusCode = statusCode;
        return this;
    }

    @Override
    public int statusCode() {
        return this.statusCode;
    }

    @Override
    public HttpRequest request() {
        return null;
    }

    @Override
    public Optional<HttpResponse<byte[]>> previousResponse() {
        return Optional.empty();
    }

    @Override
    public HttpHeaders headers() {
        return null;
    }

    @Override
    public byte[] body() {
        return this.body.clone();
    }

    public HttpResponseClusterImpl setBody(final byte[] body){
        this.body = body.clone();
        return this;
    }

    @Override
    public Optional<SSLSession> sslSession() {
        return Optional.empty();
    }

    @Override
    public URI uri() {
        return null;
    }

    @Override
    public HttpClient.Version version() {
        return null;
    }
}
