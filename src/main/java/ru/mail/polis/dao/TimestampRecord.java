package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

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

    /**
     * Create the record with timestamp.
     *
     * @param timestamp to define the time
     * @param value to define the value
     * @param type to specify the type of the record
     */
    public TimestampRecord(final long timestamp, final ByteBuffer value,
                               final RecordType type) {
        this.timestamp = timestamp;
        this.recordType = type;
        this.value = value;
    }

    public static TimestampRecord getEmpty() {
        return new TimestampRecord(-1, null, RecordType.ABSENT);
    }

    /**
     * Convert the record from bytes.
     *
     * @param bytes to define the input bytes
     * @return timestamp record instance
     */
    public static TimestampRecord fromBytes(@Nullable final byte[] bytes) {
        if (bytes == null) {
            return new TimestampRecord(-1, null, RecordType.ABSENT);
        }
        final var buffer = ByteBuffer.wrap(bytes);
        final var recordType = RecordType.fromValue(buffer.get());
        final var timestamp = buffer.getLong();
        return new TimestampRecord(timestamp, buffer, recordType);
    }

    /**
     * Convert the record to bytes.
     *
     * @return timestamp record instance as bytes
     */
    public byte[] toBytes() {
        var valueLength = 0;
        if (isValue()) {
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

    public boolean isDeleted() {
        return recordType == RecordType.DELETED;
    }

    /**
     * Get the value only.
     *
     * @return value of the timestamp instance
     */
    public ByteBuffer getValue() throws DAOException {
        if (!isValue()) {
            throw new DAOException("Empty record has no value",
                    new RocksDBException("Failed to get value from TimestampRecord!"));
        }
        return value;
    }

    /**
     * Get the value only as bytes.
     *
     * @return value of the timestamp instance as bytes
     */
    public byte[] getValueAsBytes() throws DAOException {
        final var val = getValue().duplicate();
        final byte[] ret = new byte[val.remaining()];
        val.get(ret);
        return ret;
    }

    /**
     * Merge multiple records into one according to their timestamps.
     *
     * @param responses to define the input records
     * @return latest timestamp record instance
     */
    public static TimestampRecord merge(final List<TimestampRecord> responses) {
        if (responses.size() == 1) return responses.get(0);
        else {
            return responses.stream()
                    .filter(timestampRecord -> !timestampRecord.isEmpty())
                    .max(Comparator.comparingLong(TimestampRecord::getTimestamp))
                    .orElseGet(TimestampRecord::getEmpty);
        }
    }
}
