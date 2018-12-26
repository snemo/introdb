package introdb.heap.lock;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class LockRef extends WeakReference<LockSupport> {

    private final ReentrantReadWriteLock lock;

    public LockRef(LockSupport referent, ReferenceQueue referenceQueue, ReentrantReadWriteLock lock) {
        super(referent, referenceQueue);
        this.lock = lock;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }
}
