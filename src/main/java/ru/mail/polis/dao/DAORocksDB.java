package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DAORocksDB implements DAO {
    private RocksDB mDb;

    public DAORocksDB(RocksDB db) {
        this.mDb = db;
    }

    public static class RocksDBRecordIterator implements Iterator<Record>, AutoCloseable {

        private RocksIterator iterator;

        RocksDBRecordIterator(@NotNull RocksIterator iterator) {
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
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        var fromByteArray = from.array();

        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = mDb.newIterator();
        iterator.seek(fromByteArray);

        return new RocksDBRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final var keyByteArray = key.array();
        try {
            final var valueByteArray = mDb.get(keyByteArray);
            if (valueByteArray == null) {
                throw new NoSuchElementException("Key is not present!");
            }
            return ByteBuffer.wrap(valueByteArray);
        } catch (RocksDBException e) {
            throw new DAOException("Get method exception!", e);
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        final var keyByteArray = key.array();
        final var valueByteArray = value.array();
        try {
            mDb.put(keyByteArray, valueByteArray);
        } catch (RocksDBException  e) {
            throw new DAOException("Upsert method exception!", e);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            mDb.delete(keyByteArray);
        } catch (RocksDBException  e) {
            throw new DAOException("Remove method exception!", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            mDb.compactRange();
        } catch (RocksDBException  e) {
            throw new DAOException("Compact method exception!", e);
        }
    }

    @Override
    public void close() {
        mDb.close();
    }

    static DAO create(File data) throws IOException {
        try {
            var options = new Options();
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            var comparator = new BytewiseComparator(new ComparatorOptions());
            //options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            options.setComparator(comparator);
            RocksDB db = RocksDB.open(options, data.getAbsolutePath());
            return new DAORocksDB(db);
        } catch (RocksDBException e) {
            throw new DAOException("LevelDB instantiation failed!", e);
        }
    }

}
