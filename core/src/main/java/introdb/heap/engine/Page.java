package introdb.heap.engine;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

class Page {

    private final int number;
    private final int maxSize;
    private Set<Record> records;

    Page(int number, int maxSize) {
        this.number = number;
        this.maxSize = maxSize;
        records = new HashSet<>();
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

    void addRecord(Record record) {
        if (!willFit(record)) {
            throw new IllegalArgumentException("Record is too big for current size of the page.");
        }
        this.records.add(record);
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

    Optional<Record> getRecord(byte[] key) {
        return records.stream()
                .filter(it -> it.equalsKey(key) && !it.isDeleted())
                .findAny();
    }
}
