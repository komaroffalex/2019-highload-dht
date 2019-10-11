package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DAORocksDB implements DAO {
    private RocksDB mDb;

    private static volatile boolean open = false;
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private static WriteOptions wOptions;
    private static FlushOptions fOptions;
    private final Set<RocksDBRecordIterator> openIterators = Collections.synchronizedSet(new HashSet<RocksDBRecordIterator>());

    public DAORocksDB(RocksDB db) {
        this.mDb = db;
    }

    public static class RocksDBRecordIterator implements Iterator<Record>, AutoCloseable {

        private RocksIterator iterator;

        RocksDBRecordIterator(@NotNull RocksIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public synchronized boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public synchronized  Record next() throws IllegalStateException {
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
        public synchronized void close() {
            iterator.close();
        }
    }

    @NotNull
    @Override
    public synchronized Iterator<Record> iterator(@NotNull ByteBuffer from) {
        var fromByteArray = from.array();
        RocksIterator iterator = mDb.newIterator();
        iterator.seek(fromByteArray);
        var rocksDBRecIter = new RocksDBRecordIterator(iterator);
        openIterators.add(rocksDBRecIter);
        return rocksDBRecIter;
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer keys) throws IOException, NoSuchElementException {
        LOCK.readLock().lock();
        final ByteBuffer copy = keys.duplicate();
        final byte[] keyByteArray = new byte[copy.remaining()];
        copy.get(keyByteArray);
        try {
            final var valueByteArray = mDb.get(keyByteArray);
            if (valueByteArray == null) {
                throw new NoSuchElementException("Key is not present!");
            }
            return ByteBuffer.wrap(valueByteArray);
        } catch (RocksDBException e) {
            throw new DAOException("Get method exception!", e);
        }
        finally {
            LOCK.readLock().unlock();
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer keys, @NotNull ByteBuffer values) throws IOException {
        LOCK.writeLock().lock();
        final ByteBuffer copy = keys.duplicate();
        final byte[] keyByteArray = new byte[copy.remaining()];
        copy.get(keyByteArray);
        final ByteBuffer copyV = values.duplicate();
        final byte[] valueByteArray = new byte[copyV.remaining()];
        copyV.get(valueByteArray);
        try {
            mDb.put(wOptions, keyByteArray, valueByteArray);
        } catch (RocksDBException e) {
            throw new DAOException("Upsert method exception!", e);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    @Override
    public synchronized void remove(@NotNull ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            mDb.delete(wOptions,keyByteArray);
        } catch (RocksDBException  e) {
            throw new DAOException("Remove method exception!", e);
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        try {
            mDb.compactRange();
        } catch (RocksDBException  e) {
            throw new DAOException("Compact method exception!", e);
        }
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }
        open = false;
        closeOpenIterators();
        wOptions.close();
        fOptions.close();
        mDb.close();
        wOptions = null;
        fOptions = null;
        mDb = null;
    }

    private void closeOpenIterators() {
        HashSet<RocksDBRecordIterator> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        for (RocksDBRecordIterator iterator : iterators) {
            iterator.close();
        }
    }

    static DAO create(File data) throws IOException {
        try {
            var options = new Options();
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            var comparator = new BytewiseComparator(new ComparatorOptions());
            options.setComparator(comparator);
            options.setMaxBackgroundCompactions(2);
            options.setMaxBackgroundFlushes(2);
            wOptions = new WriteOptions();
            wOptions.setDisableWAL(true);
            fOptions = new FlushOptions();
            fOptions.setWaitForFlush(true);
            RocksDB db = RocksDB.open(options, data.getAbsolutePath());
            open = true;
            return new DAORocksDB(db);
        } catch (RocksDBException e) {
            throw new DAOException("RocksDB instantiation failed!", e);
        }
    }
}
