package introdb.heap.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

class LockSupportImpl implements LockSupport {

    private final ReadWriteLock lock;
    private final int pageNo;

    public LockSupportImpl(int pageNo, ReadWriteLock lock) {
        this.lock = lock;
        this.pageNo = pageNo;
    }

    @Override
    public <R> CompletableFuture<R> inReadOperation(Supplier<R> supplier) {
        lock.readLock().lock();
        var operationResult = supplier.get();
        lock.readLock().unlock();

        return completedFuture(operationResult);
    }

    @Override
    public <R> CompletableFuture<R> inWriteOperation(Supplier<R> supplier) {
        lock.writeLock().lock();
        var operationResult = supplier.get();
        lock.writeLock().unlock();

        return completedFuture(operationResult);
    }

//    @Override
//    public String toString() {
//        return "LockSupportImpl{" +
//                "pageNo=" + pageNo +
//                ", lock=" + lock +
//                '}';
//    }
}
