package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class TimestampRecordAndKey implements Comparable<TimestampRecordAndKey> {

    private final TimestampRecord val;
    private final ByteBuffer key;

    public TimestampRecordAndKey(final TimestampRecord value, final ByteBuffer key) {
        this.val = value;
        this.key = key;
    }

    public boolean isEmpty() {
        return !val.isValue();
    }

    public ByteBuffer getValue() throws DAOException {
        return val.getValue();
    }

    public ByteBuffer getKey() {
        return key;
    }

    @Override
    public int compareTo(@NotNull final TimestampRecordAndKey other) {
        if(key.compareTo(other.key) == 0) {
            return Long.compare(val.getTimestamp(), other.val.getTimestamp());
        }
        else {
            return key.compareTo(other.key);
        }
    }

    public static TimestampRecordAndKey fromBytes(final byte[] bytes) {
        final var buff = ByteBuffer.wrap(bytes);
        final var keyLength = buff.getInt();
        final var valueLength = buff.getInt();
        final var key = new byte[keyLength];
        final var value = new byte[valueLength];
        buff.get(key);
        buff.get(value);
        final var valueRecord = TimestampRecord.fromBytes(value);
        return new TimestampRecordAndKey(valueRecord, ByteBuffer.wrap(key));
    }

    public byte[] toBytes() {
        final var valueBytes = val.toBytes();
        final var keyBytes = toArray(key);
        final ByteBuffer retBuff = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length);
        retBuff.putInt(keyBytes.length);
        retBuff.putInt(valueBytes.length);
        retBuff.put(keyBytes);
        retBuff.put(valueBytes);
        return retBuff.array();
    }

    public static TimestampRecordAndKey fromKeyValue(final ByteBuffer key,
                                                         final TimestampRecord value) {
        return new TimestampRecordAndKey(value, key);
    }

    public boolean equalKeys(final TimestampRecordAndKey other) {
        return key.equals(other.key);
    }

    private byte[] toArray(@NotNull final ByteBuffer buffer) {
        final var bufferCopy = buffer.duplicate();
        final var array = new byte[bufferCopy.remaining()];
        bufferCopy.get(array);
        return array;
    }
}
