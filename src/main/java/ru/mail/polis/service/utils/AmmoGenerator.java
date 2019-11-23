package ru.mail.polis.service.utils;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class AmmoGenerator {
    private static final int VALUE_LENGTH = 512;

    @NotNull
    private static String randomKey() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    @NotNull
    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void put() throws IOException {
        final String key = randomKey();
        final byte[] value = randomValue();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + "\r\n");
            writer.write("\r\n");
        }
        request.write(value);
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    public static void get() throws IOException {
        final String key = randomKey();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("\r\n");
        }
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 2) {
            System.err.println("Usage:\n\tjava -cp build/classes/java/main ru.mail.polis.service.<login>.AmmoGenerator <put|get> <requests>");
            System.exit(-1);
        }

        final String mode = args[0];
        final int requests = Integer.parseInt(args[1]);

        switch (mode) {
            case "put":
                for(int i = 0; i < requests; i++) {
                    put();
                }
                break;
            case "get":
                for(int i = 0; i < requests; i++) {
                    get();
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }
    }
}
