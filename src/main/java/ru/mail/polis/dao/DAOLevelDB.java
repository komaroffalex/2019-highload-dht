package ru.mail.polis.dao;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import org.jetbrains.annotations.NotNull;
import org.iq80.leveldb.*;;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DAOLevelDB implements DAO {
    private DB mDb;

    public DAOLevelDB(DB db) {
        this.mDb = db;
    }

    public static class LevelDBRecordIterator implements Iterator<Record>, AutoCloseable {

        private DBIterator iterator;

        LevelDBRecordIterator(@NotNull DBIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Record next() throws IllegalStateException {
            if (!hasNext()) {
                throw new IllegalStateException("Iterator is not viable!");
            }
            final var keyByteArray = iterator.peekNext().getKey();
            final var valueByteArray = iterator.peekNext().getValue();
            final var key = ByteBuffer.wrap(keyByteArray);
            final var value = ByteBuffer.wrap(valueByteArray);
            final var record = Record.of(key, value);
            iterator.next();
            return record;
        }

        @Override
        public void close() throws IOException {
            try {
                iterator.close();
            }
            catch (IOException e) {
                throw new IOException("Failed to close the iterator!", e);
            }
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        var fromByteArray = from.array();

        ReadOptions readOptions = new ReadOptions().snapshot(mDb.getSnapshot());
        DBIterator iterator = mDb.iterator(readOptions);
        iterator.seek(fromByteArray);

        return new LevelDBRecordIterator(iterator);
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
        } catch (DBException e) {
            throw new DAOException("Get method exception!", e);
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        final var keyByteArray = key.array();
        final var valueByteArray = value.array();
        try {
            mDb.put(keyByteArray, valueByteArray);
        } catch (DBException e) {
            throw new DAOException("Upsert method exception!", e);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            mDb.delete(keyByteArray);
        } catch (DBException e) {
            throw new DAOException("Remove method exception!", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            mDb.compactRange(null, null);
        } catch (DBException e) {
            throw new DAOException("Compact method exception!", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            mDb.close();
        } catch (IOException e) {
            throw new IOException("Failed to close the DB!", e);
        }
    }

    static DAO create(File data) throws IOException {
        try {
            var options = new Options();
            options.createIfMissing(true);
            options.verifyChecksums(true);
            options.errorIfExists(false);
            options.compressionType(CompressionType.NONE);
            DB db = factory.open(data,options);
            return new DAOLevelDB(db);
        } catch (DBException e) {
            throw new DAOException("LevelDB instantiation failed!", e);
        }
    }

}
