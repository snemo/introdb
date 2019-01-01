package introdb.heap.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class IOController {

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

    void write(Page page) {
        try {
            fileChannel.position((long)page.number() * page.maxSize());
            fileChannel.write(page.toByteBuffer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Page findPage(int no) {
        try {
            var byteBuffer = ByteBuffer.allocateDirect(config.pageSize());
            fileChannel.read(byteBuffer, calcPosition(no));
            return Page.of(no, config.pageSize(), byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long calcPosition(int pageNo) {
        long newPosition = pageNo * config.pageSize();
        return newPosition < 0 ? 1 : newPosition;
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
