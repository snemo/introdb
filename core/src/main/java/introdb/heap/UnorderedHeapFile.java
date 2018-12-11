package introdb.heap;

import introdb.heap.engine.Config;
import introdb.heap.engine.Engine;
import introdb.heap.engine.EngineFactory;
import introdb.heap.engine.Record;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;

import static introdb.heap.utils.SerializationUtils.deserialize;
import static introdb.heap.utils.SerializationUtils.*;
import static java.util.Objects.isNull;

/**
 * Implementation of the DB engine is in introdb.engine.fch package (FileChannel).
 * Rest of the implementations (Memory Mapped Files or memory) are just playground.
 */
class UnorderedHeapFile implements Store {

    private final Engine engine;

	UnorderedHeapFile(Path path, int maxNrPages, int pageSize) throws IOException{
		engine = EngineFactory.createEngine(path, maxNrPages, pageSize);
    }

	@Override
	synchronized public void put(Entry entry) throws IOException, ClassNotFoundException {
		engine.put(serialize(entry.key()), serialize(entry.value()));
	}
	
	@Override
	synchronized public Object get(Serializable key) throws IOException, ClassNotFoundException {
		var valueBytes = engine.get(serialize(key))
                .map(Record::value)
                .orElse(null);

        return isNull(valueBytes) ? null : deserialize(valueBytes);
	}
	
	synchronized public Object remove(Serializable key) throws IOException, ClassNotFoundException {
		var valueBytes = engine.remove(serialize(key))
                .map(Record::value)
                .orElse(null);

		return isNull(valueBytes) ? null : deserialize(valueBytes);
	}
}

