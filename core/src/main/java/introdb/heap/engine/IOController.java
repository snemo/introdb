package introdb.heap.engine;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class IOController implements Iterable<Page> {

    private final Config config;
    private FileChannel fileChannel;

    IOController(Config config) {
        this.config = config;
    }

    static IOController of(Path path, int maxNrPages, int pageSize) {
        return new IOController(Config.of(path, pageSize, maxNrPages));
    }

    void init() throws IOException {
        fileChannel = FileChannel.open(config.path(), READ, WRITE);
    }

    Config config() {
        return config;
    }

    @Override
    public Iterator<Page> iterator() {
        return new PageIteratorImpl(fileChannel, config);
    }

    void write(Page page) throws IOException {
        fileChannel.position(page.number() * page.maxSize());
        fileChannel.write(page.toByteBuffer());
    }

    public Page findPage(int no) throws IOException {
        var byteBuffer = ByteBuffer.allocateDirect(config.pageSize());
        fileChannel.read(byteBuffer, calcPosition(no));
        return Page.of(no, config.pageSize(), byteBuffer);
    }

    private long calcPosition(int pageNo) {
        long newPosition = pageNo * config.pageSize();
        return newPosition < 0 ? 1 : newPosition;
    }

    private static class PageIteratorImpl implements Iterator<Page> {

        private final FileChannel fileChannel;
        private final Config config;
        private ByteBuffer byteBuffer;
        private boolean pageProcessed = true;
        private int pageNo = 0;

        PageIteratorImpl(FileChannel fileChannel, Config config) {
            this.fileChannel = fileChannel;
            this.config = config;
            init();
        }

        @Override
        public boolean hasNext() {
            loadNextChunk();
            byteBuffer.rewind();
            return byteBuffer.getShort(0) > 0;
        }

        @Override
        public Page next() {
            loadNextChunk();
            byteBuffer.rewind();
            var page = Page.of(pageNo, config.pageSize(), byteBuffer);
            pageNo += 1;
            pageProcessed = true;
            return page;
        }

        private void init() {
            try {
                fileChannel.position(0); // FIXME:
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        private void loadNextChunk() {
            if (pageProcessed) {
                try {
                    byteBuffer = ByteBuffer.allocateDirect(config.pageSize());
                    fileChannel.read(byteBuffer, calcPosition());
                    pageProcessed = false;
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        private long calcPosition() {
            long newPosition = pageNo * config.pageSize();
            return newPosition < 0 ? 1 : newPosition;
        }
    }

    static class Config {
        private final int pageSize;
        private final int maxNrPages;
        private final Path path;

        private Config(Path path, int pageSize, int maxNrPages) {
            this.pageSize = pageSize;
            this.maxNrPages = maxNrPages;
            this.path = path;
        }

        public static Config of(Path path, int pageSize, int maxNrPages) {
            return new Config(path, pageSize, maxNrPages);
        }

        int pageSize() {
            return pageSize;
        }

        int MaxNrPages() {
            return maxNrPages;
        }

        public Path path() {
            return path;
        }
    }
}
