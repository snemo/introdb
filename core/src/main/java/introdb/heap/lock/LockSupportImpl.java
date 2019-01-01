package introdb.heap.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

class LockSupportImpl implements LockSupport {

    private final CompletableFuture<ReentrantReadWriteLock> lockFuture;
    private final LongAdder counter = new LongAdder();
    private volatile ReentrantReadWriteLock lock;

    public LockSupportImpl(CompletableFuture<ReentrantReadWriteLock> lockFuture) {
        this.lockFuture = lockFuture;
    }

    @Override
    public <R> CompletableFuture<R> inReadOperation(Supplier<R> supplier) {
        return lockFuture.thenApply((lock) -> {
            setLockIfEmpty(lock);
            return lockOperation(lock.readLock(), supplier);
        });
    }

    @Override
    public <R> CompletableFuture<R> inWriteOperation(Supplier<R> supplier) {
        return lockFuture.thenApply((lock) -> {
            setLockIfEmpty(lock);
            return lockOperation(lock.writeLock(), supplier);
        });
    }

    public synchronized boolean isEligibleToCollect() {
        return lock != null && counter.sum() <= 0;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    private <R> R lockOperation(Lock lock, Supplier<R> supplier) {
        counter.increment();
        R operationResult;
        try {
            lock.lock();
            operationResult = supplier.get();
        } finally {
            lock.unlock();
            counter.decrement();
        }
        return operationResult;
    }

    private void setLockIfEmpty(ReentrantReadWriteLock lock) {
        if (this.lock == null && lock != null) {
            this.lock = lock;
        }
    }
}
