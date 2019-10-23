package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;
import org.rocksdb.CompressionType;
import org.rocksdb.BuiltinComparator;

import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Iterator;

public final class DAORocksDB implements DAO {
    private final RocksDB mdb;

    private static WriteOptions wOptions;
    private final Object objLock = new Object();

    private DAORocksDB(final RocksDB db) {
        this.mdb = db;
    }

    public static class RocksDBRecordIterator implements Iterator<Record>, AutoCloseable {

        private final RocksIterator iterator;

        RocksDBRecordIterator(@NotNull final RocksIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Record next() throws IllegalStateException {
            if (!hasNext()) {
                throw new IllegalStateException("Iterator is not viable!");
            }
            final var keyByteArray = iterator.key();
            final ByteBuffer unpackedKey = compressKey(keyByteArray);
            final var valueByteArray = iterator.value();
            final var value = ByteBuffer.wrap(valueByteArray);
            final var record = Record.of(unpackedKey, value);
            iterator.next();
            return record;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final RocksIterator iterator = mdb.newIterator();
        final byte[] packedKey = decompressKey(from);
        iterator.seek(packedKey);
        return new RocksDBRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer keys) throws IOException, NoSuchElementException {
        synchronized (objLock) {
            try {
                final byte[] packedKey = decompressKey(keys);
                final byte[] valueByteArray = mdb.get(packedKey);
                if (valueByteArray == null) {
                    throw new NoSuchElementException("Key is not present!");
                }
                return ByteBuffer.wrap(valueByteArray);
            } catch (RocksDBException e) {
                throw new DAOException("Get method exception!", e);
            }
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer keys, @NotNull final ByteBuffer values) throws IOException {
        synchronized (objLock) {
            try {
                final byte[] packedKey = decompressKey(keys);
                final byte[] arrayValue = copyAndExtractFromByteBuffer(values);
                mdb.put(wOptions,packedKey, arrayValue);
            } catch (RocksDBException e) {
                throw new DAOException("Upsert method exception!", e);
            }
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] packedKey = decompressKey(key);
            mdb.delete(wOptions,packedKey);
        } catch (RocksDBException e) {
            throw new DAOException("Remove method exception!", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            mdb.compactRange();
        } catch (RocksDBException e) {
            throw new DAOException("Compact method exception!", e);
        }
    }

    @Override
    public void close() throws DAOException {
        try {
            mdb.syncWal();
            mdb.closeE();
        } catch (RocksDBException exception) {
            throw new DAOException("Error while close", exception);
        }
    }

    static DAO create(final File data) throws IOException {
        try {
            final var options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            options.setMaxBackgroundCompactions(2);
            options.setMaxBackgroundFlushes(2);
            wOptions = new WriteOptions();
            wOptions.setDisableWAL(true);
            final RocksDB db = RocksDB.open(options, data.getAbsolutePath());
            return new DAORocksDB(db);
        } catch (RocksDBException e) {
            throw new DAOException("RocksDB instantiation failed!", e);
        }
    }

    private static byte[] copyAndExtractFromByteBuffer(@NotNull final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }

    private static byte[] decompressKey(@NotNull final ByteBuffer key) {
        final byte[] arrayKey = copyAndExtractFromByteBuffer(key);
        for (int i = 0; i < arrayKey.length; i++) {
            arrayKey[i] -= Byte.MIN_VALUE;
        }
        return arrayKey;
    }

    private static ByteBuffer compressKey(@NotNull final byte[] key) {
        final byte[] copy = Arrays.copyOf(key, key.length);
        for (int i = 0; i < copy.length; i++) {
            copy[i] += Byte.MIN_VALUE;
        }
        return ByteBuffer.wrap(copy);
    }
}
