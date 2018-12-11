package introdb.heap.engine;

import java.io.IOException;
import java.nio.file.Path;

public class EngineFactory {
    public static Engine createEngine(Path path, int maxNrPages, int pageSize) throws IOException {
        var config = Config.of(pageSize, maxNrPages);
        var fileController = new FileController(config, path);
        fileController.init();
        return Engine.of(config, new PageReader(fileController), new PageWriter(fileController));
    }
}
