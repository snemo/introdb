package introdb.heap.engine;

import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

class Page {

    private final int number;
    private final int maxSize;
    private final CopyOnWriteArrayList<Record> records = new CopyOnWriteArrayList<>();

    Page(int number, int maxSize) {
        this.number = number;
        this.maxSize = maxSize;
    }

    static Page of(int number, int maxSize) {
        var page = new Page(number, maxSize);
        return page;
    }

    static Page of(int number, int maxSize, Record record) {
        var page = new Page(number, maxSize);
        page.addRecord(record);
        return page;
    }

    static Page of(int number, int maxSize, ByteBuffer byteBuffer) {
        var page = new Page(number, maxSize);
        var pageOffset = 0;

        while (Record.exists(byteBuffer, pageOffset)) {
            var record = Record.of(byteBuffer, pageOffset);
            page.addRecord(record);
            pageOffset += record.size();
        }

        return page;
    }

    // this need to be synchronized -> critical section!
    synchronized boolean addRecord(Record record) {
        if (!willFit(record)) {
            return false;
        }

        return this.records.add(record);
    }

    ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(maxSize);
        records.stream()
                .forEach(record -> record.append(byteBuffer));

        return byteBuffer.rewind();
    }

    int size() {
        return records.stream()
                .mapToInt(Record::size)
                .sum();
    }

    int maxSize() {
        return maxSize;
    }

    int number() {
        return number;
    }

    boolean willFit(Record record) {
        return (maxSize - size()) > record.size();
    }

    boolean contains(byte[] key) {
        return records.stream()
                .anyMatch(it -> it.equalsKey(key) && !it.isDeleted());
    }

    Record getRecord(byte[] key) {
        for (Record record : records) {
            if (record.equalsKey(key) && !record.isDeleted()) {
                return record;
            }
        }
        return null;
    }
}
