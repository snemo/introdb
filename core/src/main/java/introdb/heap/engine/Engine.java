package introdb.heap.engine;

import introdb.heap.utils.ByteArrayWrapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.isNull;

/**
 * InnoDB engine implementation based on FileChannel
 *
 *  Engine is using buffers for last page and currently read page.
 *
 * @author snemo
 */
public class Engine {

    private final IOController ioController;

    // Index as a mapping between key and page number
    private final ConcurrentMap<ByteArrayWrapper, Integer> index = new ConcurrentHashMap<>();

    private volatile Page lastPage;          // buffer
    private volatile Page currentPage;       // buffer

    private Engine(IOController ioController) throws IOException {
        this.ioController = ioController;
        init();
    }

    static Engine of(IOController ioController) throws IOException {
        return new Engine(ioController);
    }

    public static Engine of(Path path, int maxNrPages, int pageSize) throws IOException {
        return new Engine(IOController.of(path, maxNrPages, pageSize));
    }

    public void init() throws IOException {
        ioController.init();
        lastPage = Page.of(0, ioController.config().pageSize());
    }

    public synchronized void put(byte[] key, byte[] value) throws IOException {
        remove(key);

        var record = Record.of(key, value);

        if (lastPage.willFit(record)) {
            lastPage.addRecord(record);
        } else {
            ioController.write(lastPage);
            lastPage = Page.of(lastPage.number()+1, ioController.config().pageSize(), record);
        }

        index.put(ByteArrayWrapper.of(key), lastPage.number());
    }

    public synchronized Record remove(byte[] key) throws IOException {
        var page = findPageByIndex(key);

        // if page wasn't find for this key
        if (page == null) {
            return null;
        }

        // delete and flush
        var record = page.getRecord(key);
        if (record != null) {
            record.delete();
            ioController.write(page);
        }

        return record;
    }

    public synchronized Record get(byte[] key) throws IOException {
        var page = findPageByIndex(key);
        return isNull(page) ? null : page.getRecord(key);
    }

    private Optional<Page> getPage(byte[] key) throws IOException {
        var bufferPage = getPageFromBuffer(key);
        if (bufferPage.isPresent()){
            return bufferPage;
        }

        Iterator<Page> iterator = ioController.iterator();
        while (iterator.hasNext()) {
            var page = iterator.next();
            if (page.contains(key)) {
                currentPage = page;
                return Optional.of(page);
            }
        }

        return Optional.empty();
    }

    private Page findPageByIndex(byte[] key) throws IOException {
        // check buffer first
        var bufferedPage = getPageFromBuffer(key);
        if (bufferedPage.isPresent()){
            return bufferedPage.get();
        }

        // find page number by key in index
        int pageNo = index.getOrDefault(ByteArrayWrapper.of(key), -1);

        // if key was not found in index, return null
        if (pageNo < 0) {
            return null;
        }

        return ioController.findPage(pageNo);
    }

    private Optional<Page> getPageFromBuffer(byte[] key) {
        return Optional.ofNullable(
                checkBuffer(currentPage, key)
                .orElseGet(()-> checkBuffer(lastPage, key).orElse(null)));
    }

    private Optional<Page> checkBuffer(Page page, byte[] key) {
        return Optional.ofNullable(page)
                .filter(p -> p.contains(key));
    }

}

