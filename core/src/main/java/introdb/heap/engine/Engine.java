package introdb.heap.engine;

import java.io.IOException;
import java.util.Optional;

/**
 * InnoDB engine implementation based on FileChannel
 *
 *  Engine is using buffers for last page and currently read page.
 *
 * @author snemo
 */
public class Engine {

    private final Config config;
    private final PageReader reader;
    private final PageWriter writer;

    private Page lastPage;          // buffer
    private Page currentPage;       // buffer

    private Engine(Config config, PageReader reader, PageWriter writer) {
        this.config = config;
        this.reader = reader;
        this.writer = writer;
        init();
    }

    static Engine of(Config config, PageReader reader, PageWriter writer) {
        return new Engine(config, reader, writer);
    }

    public void init() {
        lastPage = Page.of(0, config.pageSize());
    }

    public void put(byte[] key, byte[] value) throws IOException {
        remove(key);
        var record = Record.of(key, value);

        if (lastPage.willFit(record)) {
            lastPage.addRecord(record);
        } else {
            writer.write(lastPage);
            lastPage = Page.of(lastPage.number()+1, config.pageSize(), record);
        }
    }

    public Optional<Record> remove(byte[] key) throws IOException {
        var page = getPage(key);
        var record = page.flatMap(it -> it.getRecord(key));
        record.ifPresent(Record::delete);

        if (record.isPresent()) {
            writer.write(page.get());
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

        PageIterator iterator = reader.iterator();
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
