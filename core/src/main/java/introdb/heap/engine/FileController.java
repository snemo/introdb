package introdb.heap.engine;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class FileController {

    private final Config config;
    private final Path path;
    private FileChannel fileChannel;

    FileController(Config config, Path path) {
        this.config = config;
        this.path = path;
    }

    void init() throws IOException {
        fileChannel = FileChannel.open(path, READ, WRITE);
    }

    FileChannel fileChannel() {
        return fileChannel;
    }

    Config config() {
        return config;
    }

    void close() throws IOException {
        fileChannel.close();
    }
}