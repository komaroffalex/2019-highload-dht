package ru.mail.polis.service.utils;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class AmmoGenerator {
    private static final int VALUE_LENGTH = 512;
    private static final String DILIMETER = "\r\n";

    private AmmoGenerator() {

    }

    @NotNull
    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @NotNull
    private static List<String> getUniqueKeys(final int amount) {
        final List<String> keys = new ArrayList<>(amount);
        for (long i = 0; i < amount; i++) {
            keys.add(Long.toHexString(i));
        }
        return keys;
    }

    private static int getRandomNumberInRange(final int min, final int max) {
        final Random r = new Random();
        return r.ints(min, max).findFirst().getAsInt();
    }

    private static void generateUniquePuts(final int amount) throws IOException {
        final List<String> keys = getUniqueKeys(amount);
        for (final String key : keys) {
            final byte[] value = randomValue();
            putKeyVal(key, value);
        }
    }

    private static void generatePartialOverwritePuts(final int amount) throws IOException {
        final List<String> keys = getUniqueKeys(amount);
        final Double amountRepeat = keys.size() * 0.1;
        final int amountToRepeat = amountRepeat.intValue();
        for (int i = 0; i < amountToRepeat; i++) {
            keys.set(getRandomNumberInRange(0, amount), keys.get(getRandomNumberInRange(0, amount)));
        }
        for (final String key : keys) {
            final byte[] value = randomValue();
            putKeyVal(key, value);
        }
    }

    private static void generateExistingGets(final int amount) throws IOException {
        final List<String> keys = getUniqueKeys(amount);
        Collections.shuffle(keys);
        for (final String key : keys) {
            getKey(key);
        }
    }

    private static void generateExistingGetsNewestFirst(final int amount) throws IOException {
        final List<String> keys = getUniqueKeys(amount);
        final Random r = new Random();
        for (int i = 0; i < amount; i++) {
            final double val = r.nextGaussian() * amount * 0.1 + amount * 0.9;
            int keyInd = (int) Math.round(val);
            if (keyInd >= amount) {
                keyInd = amount - 1;
            } else if (keyInd < 0) {
                keyInd = 0;
            }
            getKey(keys.get(keyInd));
        }
    }

    private static void generateMixedPutsAndGets(final int amount) throws IOException {
        final int halfAmount = amount / 2;
        final List<String> keys = getUniqueKeys(halfAmount);
        final List<String> puttedKeys = new ArrayList<>();
        for (int i = 0; i < halfAmount; i++) {
            final int choice = getRandomNumberInRange(0, 2);
            if (choice == 0) {
                final byte[] value = randomValue();
                putKeyVal(keys.get(0), value);
                puttedKeys.add(keys.get(0));
            } else if (choice == 1 && puttedKeys.size() > 2) {
                final int getIdx = getRandomNumberInRange(0, puttedKeys.size());
                getKey(puttedKeys.get(getIdx));
            }
        }
    }

    private static void putKeyVal(final String key, final byte[] value) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1" + DILIMETER);
            writer.write("Content-Length: " + value.length + DILIMETER);
            writer.write(DILIMETER);
        }
        request.write(value);
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write(DILIMETER.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Output formatted request.
     *
     * @param key to define key in the request
     */
    private static void getKey(final String key) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1" + DILIMETER);
            writer.write(DILIMETER);
        }
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write(DILIMETER.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Parse the command-line input arguments and produce the requests.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage:\n\tjava -cp build/classes/java/main ru.mail.polis.service.<login>"
                    + ".AmmoGenerator <put|get> <requests>");
            System.exit(-1);
        }

        final String mode = args[0];
        final int requests = Integer.parseInt(args[1]);

        switch (mode) {
            case "puts_unique":
                generateUniquePuts(requests);
                break;
            case "puts_overwrite":
                generatePartialOverwritePuts(requests);
                break;
            case "gets_existing":
                generateExistingGets(requests);
                break;
            case "gets_latest":
                generateExistingGetsNewestFirst(requests);
                break;
            case "mixed":
                generateMixedPutsAndGets(requests);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }
    }
}
