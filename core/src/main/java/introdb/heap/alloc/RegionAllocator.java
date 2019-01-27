package introdb.heap.alloc;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class RegionAllocator {

	private static final Logger LOG = Logger.getLogger(RegionAllocator.class.getName());

	private ConcurrentSkipListSet<Region> freeRegions = new ConcurrentSkipListSet<>();

	private final int maxRegionSize;
	private final int minRegionSize;
	private final long heapMaxSize;

	private AtomicLong allocatedMemory = new AtomicLong(0);
	private AtomicInteger allocatedPages = new AtomicInteger(-1);

	public RegionAllocator(int initialNumberOfRegions, int maxRegionSize, int minRegionSize) {
		this.maxRegionSize = maxRegionSize;
		this.minRegionSize = minRegionSize;

		this.heapMaxSize = ((long) initialNumberOfRegions) * maxRegionSize;
	}

	Optional<Region> alloc(int size) {
		if (size > maxRegionSize) {
			return Optional.empty();
		}
		if (size < minRegionSize) {
			size = minRegionSize;
		}

		// increase allocated memory size before obtaining the region
		// we will be sure that nobody else(other thread) will not steal heap space
		long currentMemorySize;
		for (;;) {
			if ((currentMemorySize = allocatedMemory.get()) + size > heapMaxSize) {
				LOG.warning("Not enough free space in heap to allocate new region");
				return Optional.empty();
			}
			if (allocatedMemory.compareAndSet(currentMemorySize, currentMemorySize + size))  // A -> B -> A problem is not important here
				break;
		}

		for (;;) {
			// check first if there is a available region in free region pool
			Optional<Region> region = findRegion(size);
			if (region.isPresent()) {
				return region;
			}

			int lastPage = allocatedPages.get(), nextPage = lastPage + 1;
			// create a new "page" and put rest of the page memory into free region pool
			region = Optional.of(new Region(nextPage, 0, size));
			Region freeRegion = new Region(nextPage, size, maxRegionSize - size);

			if (allocatedPages.compareAndSet(lastPage, nextPage)) {
				freeRegions.add(freeRegion);
				return region;
			}
		}
	}
		
	void free(Region region) {
		Region nextRegion = freeRegions.ceiling(region);
		if (nextRegion != null && nextRegion.pageNr() == region.pageNr()
				&& region.offset()+region.size() == nextRegion.offset()) {
			freeRegions.remove(nextRegion);
			freeRegions.add(new Region(region.pageNr(), region.offset(), region.size() + nextRegion.size()));
		} else {
			freeRegions.add(region);
		}
		allocatedMemory.set(allocatedMemory.get()-region.size());
	}

	private Optional<Region> findRegion(int size) {
		// first match
		return freeRegions.stream()
				.filter(r -> r.size() >= size)
				.findAny()
				.map(r -> {
					if (r.size() == size) {
						return r;
					}
					freeRegions.remove(r);
					freeRegions.add(new Region(r.pageNr(), r.offset()+size, r.size() - size));
					return new Region(r.pageNr(), r.offset(), size);
				});
	}
	
}
