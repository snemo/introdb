package introdb.heap.lock;

import introdb.heap.pool.ObjectFactory;
import introdb.heap.pool.ObjectPool;

import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {


	private final ConcurrentMap<Integer, ReentrantReadWriteLock> locksInUse = new ConcurrentHashMap<>();

	private final ConcurrentMap<Integer, LockRef> lockSupportInUse = new ConcurrentHashMap<>();

	private final ReferenceQueue<LockSupport> lockRefQueue = new ReferenceQueue<>();


	private ObjectPool<ReentrantReadWriteLock> objectPool;



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

		// first try to get find lock (LockSupport) which could be in use
		var lockSupport = getLockSupportInUse(i);

		// if there is no any locks in use for this page, create a new one

		if (lockSupport == null) {
			var lock = objectPool.borrowObject().join();
			lockSupport = new LockSupportImpl(i, lock); // FIXME:
			var lockRef = new LockRef(lockSupport, lockRefQueue, lock);
			lockSupportInUse.put(i, lockRef);
		}


		return lockSupport;
	}

	public void shutdown() throws Exception{
		
	}

	// thread safe
	private LockSupport getLockSupportInUse(int i) {
		var lockRef = lockSupportInUse.get(i);
		if ( lockRef != null ) {
			return lockRef.get();
		}
		return null;
	}


	private ReentrantReadWriteLock obtainLock(int key, ReentrantReadWriteLock lockInUse) {
		if (lockInUse != null) {
			return lockInUse;
		}

		return objectPool.borrowObject().join();
	}

	// not thread safe
	private void reclaimLock() {
		LockRef lockRef;
		while ( (lockRef =  (LockRef) lockRefQueue.poll()) != null) {
			objectPool.returnObject(lockRef.getLock());
		}
	}


}
