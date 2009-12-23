package myDB;

import java.util.List;

/**
 * An java implementation of Extendible Hashing Index
 * @author attran
 *
 */
public class MyExtHashIndex implements HashIndirectIndex {

	/** Number of slots in one bucket. Need experimental evaluations */
	private static final int BUCKET_SIZE = 8;

	/** The initial capacity of data entries, need experimental evaluation */
	private static final int INITIAL_CAPACITY = 12;

	/** The expanding factor of a data entry, need experimental evaluation */
	private static final float FACTOR = 1.5f;

	/** Current global depth */
	private int gDepth;

	/** Lists of buckets, of which items are an array of size BUCKET_SIZE.
	 * Items of an array is an array-liked list with key's hashCode at the
	 * beginning, followed by rowIDs in ascending order */
	private List buckets;

	/** 
	 * The local depths, and also the implicit directory. To be double
	 * when needed 
	 */
	private int[] lDepths;

	/** 
	 * List keeping track of the last occupied slot in buckets. If one
	 * bucket is full, its corresponding freeSlot value will be BUCKET_SIZE 
	 */
	private int[] freeSlot;

	/**
	 * Return the buckets no. which contains the key
	 */
	private int hash(int key) {
		return MyHashFunctions.hash32shiftmult(key)	% gDepth;	
	}

	public void insert(Object objKey, int rowID) {
		int key = objKey.hashCode();
		int bucketNo = hash(key);
		Object[] bucket = (Object[])buckets.get(bucketNo);
		int i = freeSlot[bucketNo];
		
		/**
		 * Step 1: Try to add into the matching bucket. Stop if successful
		 * Due to the way data entries are organized, we need to perform
		 * binary search over the keys, and within every data entry - over
		 * the list of rowIDs.
		 * 
		 */
		int low = 0, high = i - 1;
		int[] entry;				
		for (; low <= high;) {
			int mid = (low + high) >> 1;
			entry = (int[])bucket[mid];
			int tmpKey = entry[0];		//The beginning elements is key content
			if (tmpKey < key) 
				low = mid + 1;
			else if (tmpKey > key)
				high = mid - 1;
			// Key found, just insert the value into the matching data entry.
			else {				
				int entrySize = entry[1]; 	//The second elements are data entry size
				int low1 = 2, high1 = entrySize - 1;
				for (; low1 <= high1;) {
					int mid1 = (low1 + high1) >> 1;
					int midVals = entry[mid1];
					if (midVals < rowID)
						low1 = mid1 + 1;
					else if (midVals > rowID)
						high1 = mid1 - 1;
					else return;		//value found, do nothing			
				}
				// Value not found, insert new value into current data entry.
				// Firs check for free slots in the entry. Expand entry if full
				if (entrySize == entry.length) {
					int[] newEntry = new int[Math.round(entrySize * FACTOR)];
					System.arraycopy(entry, 0, newEntry, 0, entrySize);
					entry = newEntry;
					bucket[mid] = entry;
				}
				// Then, insert value into current position
				if (low1 < entrySize) {
					System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
					entry[low1] = rowID;
				}
				else entry[entrySize] = rowID;
				entry[1]++;				
				return;
			}
		}
		/**
		 * Step 2: Key is new, try to add it into the bucket.
		 */
		
		// Create a data entry in format: [key,size,list of rowID]
		entry = new int[INITIAL_CAPACITY];
		entry[0] = key;
		entry[1] = 3; 
		entry[2] = rowID;
		
		if (i < BUCKET_SIZE) {	
			if (low < i) {
				System.arraycopy(bucket, low, bucket, low + 1, i - low);			
				bucket[low] = entry;
			}
			else bucket[i] = entry;			
			freeSlot[bucketNo]++;
			return;
		}
		
		/** 
		 * Step 3: Adding failed, do extendible hashing 
		 */
		else {
			Object[] oldBucket = bucket.clone();			
			Object tmp;
			if (gDepth > lDepths[bucketNo]) {
				lDepths[bucketNo]++;	
				//bucket[freeSlot[bucketNo]++] = entry;
			}		
			else {
				lDepths[bucketNo] = ++gDepth;
				
				//Double the directory
				i = lDepths.length;
				high = i * 2;
				int[] newLDepths = new int[high];
				System.arraycopy(lDepths, 0, newLDepths, 0, i);
				lDepths = newLDepths;
				
				int[] newFreeSlot = new int[high];
				System.arraycopy(freeSlot, 0, newFreeSlot, 0, i);
				freeSlot = newFreeSlot;
				
				while (i++ < high)
					buckets.add(MyNull.NULLOBJ);
				
				//Rehash the key
				bucketNo = hash(key);
				tmp = buckets.get(bucketNo);
				
				if (tmp != MyNull.NULLOBJ) {
					bucket = (Object[])tmp;
					bucket[freeSlot[bucketNo]++] = entry;
				}					
				else {
					lDepths[bucketNo] = gDepth;
					bucket = new Object[BUCKET_SIZE];
					bucket[freeSlot[bucketNo]++] = entry;
					buckets.set(bucketNo, bucket);
				}				
			}
			
			//Rehash other keys in the old bucket
			for (i = 0; i < BUCKET_SIZE; i++) { 
				bucketNo = hash(((int[])oldBucket[i])[0]);
				tmp = buckets.get(bucketNo);
				if (tmp != MyNull.NULLOBJ) {
					bucket = (Object[])tmp;
					bucket[freeSlot[bucketNo]++] = oldBucket[i];
				}					
				else {
					lDepths[bucketNo] = gDepth;
					bucket = new Object[BUCKET_SIZE];
					bucket[freeSlot[bucketNo]++] = oldBucket[i];
					buckets.set(bucketNo, bucket);
				}
			}
		}
	}

	@Override
	public int[] pointQueryRowIDs(Object objKey) {
		int key = objKey.hashCode();
		int bucketNo = hash(key);
		Object[] bucket = (Object[])buckets.get(bucketNo);
		int[] entry = null;
		
		int i = freeSlot[bucketNo];
		int low = 0, high = i - 1;
						
		for (; low <= high;) {
			int mid = (low + high) >> 1;
			entry = (int[])bucket[mid];
			int tmpKey = entry[0];		//The beginning elements is key content
			if (tmpKey < key) 
				low = mid + 1;
			else if (tmpKey > key)
				high = mid - 1;
			// Key found, just retrieve the values in entry from 3rd elements on
			else {					
				System.arraycopy(entry, 2, entry, 0, entry.length - 2);
			}
		}
		return entry;
	}

	@Override
	public void delete(Object objKey, int rowID) {
		int key = objKey.hashCode();
		int bucketNo = hash(key);
		Object[] bucket = (Object[])buckets.get(bucketNo);
		
		int i = freeSlot[bucketNo];
		int low = 0, high = i - 1;
		
		int[] entry;				
		for (; low <= high;) {
			int mid = (low + high) >> 1;
			entry = (int[])bucket[mid];
			int tmpKey = entry[0];		//The beginning elements is key content
			if (tmpKey < key) 
				low = mid + 1;
			else if (tmpKey > key)
				high = mid - 1;
			// Key found, further search for the matching row and delete if found.
			else {				
				boolean found = false;
				int entrySize = entry[1]; 	//The second elements are data entry size
				int low1 = 2, high1 = entrySize - 1;
				for (; low1 <= high1;) {
					int mid1 = (low1 + high1) >> 1;
					int midVals = entry[mid1];
					if (midVals < rowID)
						low1 = mid1 + 1;
					else if (midVals > rowID)
						high1 = mid1 - 1;
					else  {
						low1 = mid1;
						found = true;		//value found, stop searching
						break;				
					}
				}
				if (found) {
					System.arraycopy(entry, low1 + 1, entry, low1, entry.length - low1 - 1);

				}
				return;
			}
		}		
		return;
	}

	@Override
	public void update(Object objKey, int oldRowID, int newRowID) {
		int key = objKey.hashCode();
		int bucketNo = hash(key);
		Object[] bucket = (Object[])buckets.get(bucketNo);
		int[] entry;
		
		int i = freeSlot[bucketNo];
		int low = 0, high = i - 1;
						
		for (; low <= high;) {
			int mid = (low + high) >> 1;
			entry = (int[])bucket[mid];
			int tmpKey = entry[0];		//The beginning elements is key content
			if (tmpKey < key) 
				low = mid + 1;
			else if (tmpKey > key)
				high = mid - 1;
			// Key found, further look for the matching row and update
			else {					
				int entrySize = entry[1]; 	//The second elements are data entry size
				int low1 = 2, high1 = entrySize - 1;
				for (; low1 <= high1;) {
					int mid1 = (low1 + high1) >> 1;
					int midVals = entry[mid1];
					if (midVals < oldRowID)
						low1 = mid1 + 1;
					else if (midVals > oldRowID)
						high1 = mid1 - 1;
					else entry[mid1] = newRowID;		//value found, update	
					return; 			
				}
				// Value not found, stop			
				return;
			}
		}
		return;		
	}	
}