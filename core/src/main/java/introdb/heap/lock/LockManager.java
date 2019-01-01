package introdb.heap.lock;

import introdb.heap.pool.ObjectFactory;
import introdb.heap.pool.ObjectPool;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

	private ObjectPool<ReentrantReadWriteLock> objectPool;
    private final ConcurrentMap<Integer, LockSupportImpl> locksInUse = new ConcurrentHashMap<>();

	public LockManager() {
		objectPool = new ObjectPool<>(ReentrantReadWriteLock::new,l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
	}

	// visible for testing only, so we can inject mocks
	LockManager(ObjectFactory<ReentrantReadWriteLock> lockFactory) {
		objectPool = new ObjectPool<>(lockFactory, l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
	}

	public LockSupport lockForPage(int i) {
		// piggyback, check if some locks can be released to the pool
		reclaimLock();

		// first try to find lock (LockSupport) which could be in use
		var lockSupport = locksInUse.get(i);

		// if there is no any locks in use for this page, create a new one
		if (lockSupport == null) {
			var lockFuture = objectPool.borrowObject();
			lockSupport = locksInUse.compute(i, (k, oldV) -> new LockSupportImpl(lockFuture));
		}

		return lockSupport;
	}

	public void shutdown() throws Exception{
		
	}

	private void reclaimLock() {
        locksInUse.forEach((page, lockSupport) -> { // forEach is thread safe
            if ( lockSupport.isEligibleToCollect() ) {
				locksInUse.remove(page);
                objectPool.returnObject(lockSupport.getLock());
            }
        });
	}
}
