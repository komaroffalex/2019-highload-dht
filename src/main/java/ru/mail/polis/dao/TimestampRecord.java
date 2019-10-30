package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;

public class TimestampRecord {

    private final long timestamp;
    private final ByteBuffer value;
    private final RecordType recordType;

    private enum RecordType {
        VALUE((byte) 1),
        DELETED((byte) -1),
        ABSENT((byte) 0);

        final byte value;

        RecordType(final byte value) {
            this.value = value;
        }

        static RecordType fromValue(final byte value) {
            if (value == VALUE.value) {
                return VALUE;
            } else if (value == DELETED.value) {
                return DELETED;
            } else {
                return ABSENT;
            }
        }
    }

    public TimestampRecord(final long timestamp, final ByteBuffer value,
                               final RecordType type) {
        this.timestamp = timestamp;
        this.recordType = type;
        this.value = value;
    }

    public static TimestampRecord fromBytes(@Nullable final byte[] bytes) {
        if (bytes == null) {
            return new TimestampRecord(-1, null, RecordType.ABSENT);
        }
        final var buffer = ByteBuffer.wrap(bytes);
        final var recordType = RecordType.fromValue(buffer.get());
        final var timestamp = buffer.getLong();
        return new TimestampRecord(timestamp, buffer, recordType);
    }

    public byte[] toBytes() {
        var valueLength = 0;
        if(isValue()) {
            valueLength = value.remaining();
        }
        final var byteBuff = ByteBuffer.allocate(1 + Long.BYTES + valueLength);
        byteBuff.put(recordType.value);
        byteBuff.putLong(getTimestamp());
        if (isValue()) {
            byteBuff.put(value.duplicate());
        }
        return byteBuff.array();
    }

    public static TimestampRecord fromValue(@NotNull final ByteBuffer value,
                                                final long timestamp) {
        return new TimestampRecord(timestamp, value, RecordType.VALUE);
    }

    public static boolean isEmptyRecord(@NotNull final byte[] bytes) {
        return bytes[0] != RecordType.VALUE.value;
    }

    public static TimestampRecord tombstone(final long timestamp) {
        return new TimestampRecord(timestamp, null, RecordType.DELETED);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isValue() {
        return recordType == RecordType.VALUE;
    }

    public boolean isEmpty() {
        return recordType == RecordType.ABSENT;
    }

    public ByteBuffer getValue() throws DAOException {
        if (!isValue()) {
            throw new DAOException("Empty record has no value", new RocksDBException("Failed to get value from TimestampRecord!"));
        }
        return value;
    }
}
