package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.util.BytewiseComparator;
import org.rocksdb.Options;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.NoSuchElementException;
import java.util.Iterator;

public final class DAORocksDB implements DAO {
    private final RocksDB mdb;

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
            final var valueByteArray = iterator.value();
            final var key = ByteBuffer.wrap(keyByteArray);
            final var value = ByteBuffer.wrap(valueByteArray);
            final var record = Record.of(key, value);
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
        final var fromByteArray = from.array();
        final RocksIterator iterator = mdb.newIterator();
        iterator.seek(fromByteArray);
        return new RocksDBRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer keys) throws IOException, NoSuchElementException {
        synchronized (objLock) {
        final ByteBuffer copy = keys.duplicate();
        final byte[] keyByteArray = new byte[copy.remaining()];
        copy.get(keyByteArray);
            try {
                final var valueByteArray = mdb.get(keyByteArray);
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
            final ByteBuffer copy = keys.duplicate();
            final byte[] keyByteArray = new byte[copy.remaining()];
            copy.get(keyByteArray);
            final ByteBuffer copyV = values.duplicate();
            final byte[] valueByteArray = new byte[copyV.remaining()];
            copyV.get(valueByteArray);
            try {
                mdb.put(keyByteArray, valueByteArray);
            } catch (RocksDBException e) {
                throw new DAOException("Upsert method exception!", e);
            }
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            mdb.delete(keyByteArray);
        } catch (RocksDBException  e) {
            throw new DAOException("Remove method exception!", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            mdb.compactRange();
        } catch (RocksDBException  e) {
            throw new DAOException("Compact method exception!", e);
        }
    }

    @Override
    public void close() {
        mdb.close();
    }

    static DAO create(final File data) throws IOException {
        try {
            final var options = new Options();
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            final var comparator = new BytewiseComparator(new ComparatorOptions());
            options.setComparator(comparator);
            final RocksDB db = RocksDB.open(options, data.getAbsolutePath());
            return new DAORocksDB(db);
        } catch (RocksDBException e) {
            throw new DAOException("RocksDB instantiation failed!", e);
        }
    }
}
