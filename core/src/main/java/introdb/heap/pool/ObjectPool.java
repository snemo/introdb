package introdb.heap.pool;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class ObjectPool<T> {

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private final int maxPoolSize;

	private final AtomicInteger poolSize = new AtomicInteger(0);
	private final Queue<T> freePool = new ConcurrentLinkedQueue<>();;

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this(fcty,validator,25);
	}
	
	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
		this.fcty = fcty;
		this.validator = validator;
		this.maxPoolSize = maxPoolSize;
	}
	
	/**
	 * When there is object in pool returns completed future,
	 * if not, future will be completed when object is
	 * returned to the pool.
	 * 
	 * @return
	 */
	public CompletableFuture<T> borrowObject() {
		// First try to get obj from free pool
		T obj = freePool.poll();
		if (obj != null) {
			return completedFuture(obj);
		}

		// Try to create a new object if there is still free space in main pool
		var currentPoolSize = poolSize.get();
		if (currentPoolSize < maxPoolSize) {
			poolSize.incrementAndGet();
			obj = fcty.create();
			return completedFuture(obj);
		}

		// Wait until some object will be returned
		return supplyAsync(this::waitUntilObjectFreed);
	}	
	
	public void returnObject(T object) {
		if (validator.validate(object)) {
			freePool.offer(object);
		} else {
			poolSize.decrementAndGet();
		}
	}

	public void shutdown() throws InterruptedException {
	}

	public int getPoolSize() {
		return poolSize.get();
	}

	public int getInUse() {
		return poolSize.get() - freePool.size();
	}

	private T waitUntilObjectFreed() {
		T obj;

		while (null == (obj = freePool.poll())) {
			Thread.onSpinWait();
		}

		return obj;
	}
}
