package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.WriteOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.util.BytewiseComparator;
import org.rocksdb.Options;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Iterator;

public final class DAORocksDB implements DAO {
    private RocksDB mdb;

    private static volatile boolean open;

    private static WriteOptions writeOptions;
    private static FlushOptions flushOptions;
    private final Set<RocksDBRecordIterator> openIterators = Collections.synchronizedSet(new HashSet<>());
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
        final var rocksDBRecIter = new RocksDBRecordIterator(iterator);
        openIterators.add(rocksDBRecIter);
        return rocksDBRecIter;
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer keys) throws IOException, NoSuchElementException {
        final ByteBuffer copy = keys.duplicate();
        final byte[] keyByteArray = new byte[copy.remaining()];
        copy.get(keyByteArray);
        synchronized (objLock) {
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
        final ByteBuffer copy = keys.duplicate();
        final byte[] keyByteArray = new byte[copy.remaining()];
        copy.get(keyByteArray);
        final ByteBuffer copyV = values.duplicate();
        final byte[] valueByteArray = new byte[copyV.remaining()];
        copyV.get(valueByteArray);
        synchronized (objLock) {
            try {
                mdb.put(writeOptions, keyByteArray, valueByteArray);
            } catch (RocksDBException e) {
                throw new DAOException("Upsert method exception!", e);
            }
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            mdb.delete(writeOptions,keyByteArray);
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

    private static void closeDb() {
        open = false;
    }

    @Override
    public void close() {
        if (!open) {
            return;
        }
        closeDb();
        closeOpenIterators();
        writeOptions.close();
        flushOptions.close();
        mdb.close();
        mdb = null;
    }

    private void closeOpenIterators() {
        HashSet<RocksDBRecordIterator> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        for (final RocksDBRecordIterator iterator : iterators) {
            iterator.close();
        }
    }

    static DAO create(final File data) throws IOException {
        try {
            final var options = new Options();
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            final var comparator = new BytewiseComparator(new ComparatorOptions());
            options.setComparator(comparator);
            options.setMaxBackgroundCompactions(2);
            options.setMaxBackgroundFlushes(2);
            options.setIncreaseParallelism(4);
            writeOptions = new WriteOptions();
            writeOptions.setDisableWAL(true);
            flushOptions = new FlushOptions();
            flushOptions.setWaitForFlush(true);
            final RocksDB db = RocksDB.open(options, data.getAbsolutePath());
            open = true;
            return new DAORocksDB(db);
        } catch (RocksDBException e) {
            throw new DAOException("RocksDB instantiation failed!", e);
        }
    }
}
