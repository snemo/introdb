package introdb.heap.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * InnoDB engine implementation based on FileChannel
 *
 *  Engine is using buffers for last page and currently read page.
 *
 * @author snemo
 */
public class Engine {

    private final IOController ioController;

    private Page lastPage;          // buffer
    private Page currentPage;       // buffer

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

    public void put(byte[] key, byte[] value) throws IOException {
        remove(key);
        var record = Record.of(key, value);

        if (lastPage.willFit(record)) {
            lastPage.addRecord(record);
        } else {
            ioController.write(lastPage);
            lastPage = Page.of(lastPage.number()+1, ioController.config().pageSize(), record);
        }
    }

    public Optional<Record> remove(byte[] key) throws IOException {
        var page = getPage(key);

        var record = page.flatMap(it -> it.getRecord(key));
        record.ifPresent(Record::delete);
        if (record.isPresent()) { // FIXME:
            ioController.write(page.get());
        }

        return record;
    }

    public Optional<Record> get(byte[] key) throws IOException {
        return getPage(key)
                .flatMap(page -> page.getRecord(key));
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
