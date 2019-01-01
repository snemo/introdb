package introdb.heap.engine;

import introdb.heap.lock.LockManager;
import introdb.heap.lock.LockSupport;
import introdb.heap.utils.ByteArrayWrapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * InnoDB engine implementation based on FileChannel
 *
 *  Engine is using buffers for last page and currently read page.
 *
 * @author snemo
 */
public class Engine {

    private final IOController ioController;
    private final LockManager lockManager;

    // Index as a mapping between key and page number
    private final ConcurrentMap<ByteArrayWrapper, Integer> index = new ConcurrentHashMap<>();

    // last page buffer
    private volatile AtomicReference<Page> lastPage;

    private Engine(IOController ioController, LockManager lockManager) throws IOException {
        this.ioController = ioController;
        this.lockManager = lockManager;
        init();
    }

    public static Engine of(LockManager lockManager, Path path, int maxNrPages, int pageSize) throws IOException {
        return new Engine(IOController.of(path, maxNrPages, pageSize), lockManager);
    }

    public void init() throws IOException {
        ioController.init();
        lastPage = new AtomicReference<>(Page.of(0, ioController.config().pageSize()));
    }

    public void put(byte[] key, byte[] value) throws IOException {
        var record = Record.of(key, value, ioController.config().pageSize());

        remove(key); // remove old record if exists - no duplicates

        var tmpLastPage = lastPage.get();
        var lock = lockManager.lockForPage(tmpLastPage.number());

        execute(
            lock.inWriteOperation(() -> {
                for (;;) {
                    var page = lastPage.get();

                    if (page.addRecord(record)) {
                        index.put(ByteArrayWrapper.of(key), page.number());
                        ioController.write(page);
                        break;
                    } else {
                        var newPage = Page.of(page.number()+1, ioController.config().pageSize(), record);
                        index.put(ByteArrayWrapper.of(key), newPage.number());
                        if (! lastPage.compareAndSet(page, newPage)) {
                            continue;
                        }
                        ioController.write(newPage);
                        break;
                    }
                }
                return null;
        }));
    }

    public Record remove(byte[] key) {
        // check buffer first
        var tmpLastPage = lastPage.get();
        if (tmpLastPage != null && tmpLastPage.contains(key)) {
            var lock = lockManager.lockForPage(tmpLastPage.number());
            return execute(
                    lock.inWriteOperation(() ->
                            remove(tmpLastPage, key)));
        }
        // check index
        else {
            int pageNo = index.getOrDefault(ByteArrayWrapper.of(key), -1);
            if (pageNo > -1 ) {
                var lock = lockManager.lockForPage(pageNo);
                return execute(
                        // lock sequence = writeLock.lock -> readLock.lock -> readLock.unlock() -> writeLock.unlock()
                        lock.inWriteOperation(() ->
                                remove(getPage(pageNo, lock), key)));
            }
        }

        return null;
    }

    public Record get(byte[] key) throws IOException {
        // check buffer first;
        Record record;
        if (null != (record = lastPage.get().getRecord(key))) {
            return record;
        }

        int pageNo = index.getOrDefault(ByteArrayWrapper.of(key), -1);
        if (pageNo > -1 ) {
            var lock = lockManager.lockForPage(pageNo);
            return execute(
                    lock.inReadOperation(() ->
                            ioController.findPage(pageNo).getRecord(key)));
        }

        return null;
    }

    private Record remove(Page page, byte[] key) {
        var record = page.getRecord(key);
        if (record != null) {
            index.remove(ByteArrayWrapper.of(key)); // remove from index first
            record.delete();
            ioController.write(page);
        }
        return record;
    }

    private Page getPage(int no, LockSupport lock) {
        return execute(
                lock.inReadOperation(() -> ioController.findPage(no)));
    }

    private <R> R execute(CompletableFuture<R> future) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}

