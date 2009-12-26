package myDB;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import exceptions.InvalidKeyException;

import operator.Operator;
import systeminterface.Row;
import systeminterface.Table;

import metadata.Type;
import metadata.Types;

/**
 * An java implementation of Extendible Hashing Index
 * @author attran
 *
 */
public class MyExtHashIndex implements HashIndex {

	/** Number of slots in one bucket. Need experimental evaluations */
	private static final int BUCKET_SIZE = 8;

	/** The initial capacity of data entries, need experimental evaluation */
	private static final int INITIAL_CAPACITY = 12;

	/** The expanding factor of a data entry, need experimental evaluation */
	private static final float FACTOR = 1.5f;

	/** Default global and local depth */
	private static final int DEPTH = 2;
	
	/** Default cardinality of directory */
	private static final int CARD = 4;
	
	/** Index description */
	private final String des;
		
	/** Current global depth */
	private int gDepth;
	
	/** Since Java has no exponential operator, I store the value of
	 * 2^gDepth here, to fast compute when needed */
	private int powerDepth;

	/** Lists of buckets, of which items are an array of size BUCKET_SIZE.
	 * Items of an array is an array-liked list with key's hashCode at the
	 * beginning, followed by rowIDs in ascending order 
	 */
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
	
	/** TENTATIVE -$BEGIN */
	/***
	 * Type of the real values of this index. I'm not sure if we really need
	 * this property
	 */
	private final Type type;
	
	/** The reference to base table. We might be able to do other way - I let
	 * table be of MyTable type, so that we can avoid unnecessary casting later */
	private final MyTable table;
	/** TENTATIVE -$END */

	/**
	 * Return the buckets no. which contains the key
	 */
	private int hash(int key) {
		return MyHashFunctions.hash32shiftmult(key)	& powerDepth;	
	}
	
	/** 
	 * This constructor is only for functionality testing. The real constructor
	 * should be given the column values array, obtained by calling method 
	 * MyColumn.getDataArrayAsObject() 
	 */
	public MyExtHashIndex(String indexName, Type t, Table tableObj) {
		type = t;
		table = (MyTable)tableObj;
		des = indexName;
		gDepth = DEPTH;
		powerDepth = CARD - 1;
		buckets = new ArrayList(CARD);
		lDepths = new int[CARD];
		freeSlot = new int[CARD];
		int i;
		for (i = 0; i < CARD; i++) {
			buckets.add(null);
			lDepths[i] = DEPTH;
		}
	}

	public void insert(Object objKey, int rowID) throws InvalidKeyException {
		int[] entry;			
		int key = 0;
		
		//This is really stupid
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.getLength() == ((String)objKey).length())
				key = objKey.hashCode();
			else
				throw new InvalidKeyException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}				
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		Object[] bucket;
		int i = freeSlot[bucketNo];
		/**
		 * Step 1: If this key is new, just build a data entry and add to 
		 * appropriate bucket
		 */
		if (tmp == null) {
			entry = new int[INITIAL_CAPACITY];
			entry[0] = key;
			entry[1] = 3;
			entry[2] = rowID;
			bucket = new Object[BUCKET_SIZE];
			bucket[i++] = entry;
			freeSlot[bucketNo] = i;
			buckets.set(bucketNo, bucket);
			return;
 		}
		/**
		 * Step 2: Found the bucket. Try to add into the matching bucket. 
		 * Stop if successful. Due to the way data entries are organized, 
		 * we need to perform binary search over the keys, and within every
		 * data entry - over the list of rowIDs.
		 * 
		 */
		bucket = (Object[])tmp;
		
		int low = 0, high = i - 1;
			
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
				// First check for free slots in the entry. Expand entry if full
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
		 * Step 3: Found the bucket, but key is new, then try to add it into the bucket.
		 */
		
		// Create a data entry in format: [key,size,[list of rowID]]
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
		 * Step 4: Adding failed, do extendible hashing 
		 */
		
		Object[] oldBucket = bucket.clone();			
		if (gDepth > lDepths[bucketNo]) {
			lDepths[bucketNo]++;	
			bucket[freeSlot[bucketNo]++] = entry;
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
				buckets.add(null);
			
			powerDepth = high - 1;
			
			//Rehash the key
			bucketNo = hash(key);
			tmp = buckets.get(bucketNo);
			
			if (tmp != null) {
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
			if (tmp != null) {
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


	/** Construct the index in bulk loading-liked fashion. colVals is obtained
	 * by calling MyColumn.getDataArrayAsObject() 
	 */	
	public MyExtHashIndex(String indexName, Object colVals, Type t, Table tableObj) {
		//Initialize index as usual
		type = t;
		table = (MyTable)tableObj;
		des = indexName;
		gDepth = DEPTH;
		powerDepth = CARD;
		buckets = new ArrayList(CARD);
		lDepths = new int[CARD];
		freeSlot = new int[CARD];
		
		int i;
		for (i = 0; i < CARD; i++) {
			buckets.add(null);
			lDepths[i] = DEPTH;
		}
		
		int n, r, k, bucketNo;
		int[] entry;
		Object tmp;
		Object[] bucket;
		
		boolean added;
		if(t == Types.getIntegerType()) {
			int[] keys = (int[])colVals;
			n = keys.length;
						
			for (r = 0; r < n; r++) {
				k = keys[r];
				
				if (k == Integer.MAX_VALUE || k == Integer.MIN_VALUE) 
					continue;
				
				bucketNo = hash(k);
				tmp = buckets.get(bucketNo);
				i = freeSlot[bucketNo];
				
				added = false; 
				
				/** Step 1 */
				if (tmp == null) {
					entry = new int[INITIAL_CAPACITY];
					entry[0] = k;
					entry[1] = 3;
					entry[2] = r;
					bucket = new Object[BUCKET_SIZE];
					bucket[i++] = entry;
					freeSlot[bucketNo] = i;
					buckets.set(bucketNo, bucket);
					continue;
		 		}
				
				/** Step 2 */
				bucket = (Object[])tmp;
				int low = 0, high = i - 1;
				
				for (; low <= high;) {
					int mid = (low + high) >> 1;
					entry = (int[])bucket[mid];
					int tmpKey = entry[0];		//The beginning elements is key content
					if (tmpKey < k) 
						low = mid + 1;
					else if (tmpKey > k)
						high = mid - 1;
					// Key found, just insert the value into the matching data entry.
					else {				
						int entrySize = entry[1]; 	//The second elements are data entry size
						int low1 = 2, high1 = entrySize - 1;
						boolean valFound = false;
						for (; low1 <= high1;) {
							int mid1 = (low1 + high1) >> 1;
							int midVals = entry[mid1];
							if (midVals < r)
								low1 = mid1 + 1;
							else if (midVals > r)
								high1 = mid1 - 1;
							else {
								added = true;
								break;		//value found, do nothing			
							}
						}
						if (added)
							break;
						// Value not found, insert new value into current data entry.
						// First check for free slots in the entry. Expand entry if full
						if (entrySize == entry.length) {
							int[] newEntry = new int[Math.round(entrySize * FACTOR)];
							System.arraycopy(entry, 0, newEntry, 0, entrySize);
							entry = newEntry;
							bucket[mid] = entry;
						}
						// Then, insert value into current position
						if (low1 < entrySize) {
							System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
							entry[low1] = r;
						}
						else entry[entrySize] = r;
						entry[1]++;	
						added = true;
						break;
					}
				}
				if (added)
					continue;
				
				/** Step 3 */
				entry = new int[INITIAL_CAPACITY];
				entry[0] = k;
				entry[1] = 3; 
				entry[2] = r;
				
				if (i < BUCKET_SIZE) {	
					if (low < i) {
						System.arraycopy(bucket, low, bucket, low + 1, i - low);			
						bucket[low] = entry;
					}
					else bucket[i] = entry;			
					freeSlot[bucketNo]++;
					continue;
				}
				
				/** Step 4 */
				Object[] oldBucket = bucket.clone();			
				if (gDepth > lDepths[bucketNo]) {
					lDepths[bucketNo]++;	
					bucket[freeSlot[bucketNo]++] = entry;
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
						buckets.add(null);
					
					//Rehash the key
					bucketNo = hash(k);
					tmp = buckets.get(bucketNo);
					
					if (tmp != null) {
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
					if (tmp != null) {
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
			return;
		}
		if(t == Types.getDoubleType()) {
			double[] keys = (double[])colVals;
			n = keys.length;
			double d;
			long l;
			for (r = 0; r < n; r++) {
				d = keys[r];
				if (d == Double.MAX_VALUE || d == Double.MIN_VALUE) 
					continue;
				
				l = Double.doubleToLongBits(d);
				k =  (int)(l ^ (l >>> 32));
				
				
				bucketNo = hash(k);
				tmp = buckets.get(bucketNo);
				i = freeSlot[bucketNo];

				added = false; 
				
				/** Step 1 */
				if (tmp == null) {
					entry = new int[INITIAL_CAPACITY];
					entry[0] = k;
					entry[1] = 3;
					entry[2] = r;
					bucket = new Object[BUCKET_SIZE];
					bucket[i++] = entry;
					freeSlot[bucketNo] = i;
					buckets.set(bucketNo, bucket);
					continue;
		 		}
				
				/** Step 2 */
				bucket = (Object[])tmp;
				int low = 0, high = i - 1;
				
				for (; low <= high;) {
					int mid = (low + high) >> 1;
					entry = (int[])bucket[mid];
					int tmpKey = entry[0];		//The beginning elements is key content
					if (tmpKey < k) 
						low = mid + 1;
					else if (tmpKey > k)
						high = mid - 1;
					// Key found, just insert the value into the matching data entry.
					else {				
						int entrySize = entry[1]; 	//The second elements are data entry size
						int low1 = 2, high1 = entrySize - 1;
						boolean valFound = false;
						for (; low1 <= high1;) {
							int mid1 = (low1 + high1) >> 1;
							int midVals = entry[mid1];
							if (midVals < r)
								low1 = mid1 + 1;
							else if (midVals > r)
								high1 = mid1 - 1;
							else {
								added = true;
								break;		//value found, do nothing			
							}
						}
						if (added)
							break;
						// Value not found, insert new value into current data entry.
						// First check for free slots in the entry. Expand entry if full
						if (entrySize == entry.length) {
							int[] newEntry = new int[Math.round(entrySize * FACTOR)];
							System.arraycopy(entry, 0, newEntry, 0, entrySize);
							entry = newEntry;
							bucket[mid] = entry;
						}
						// Then, insert value into current position
						if (low1 < entrySize) {
							System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
							entry[low1] = r;
						}
						else entry[entrySize] = r;
						entry[1]++;	
						added = true;
						break;
					}
				}
				if (added)
					continue;
				
				/** Step 3 */
				entry = new int[INITIAL_CAPACITY];
				entry[0] = k;
				entry[1] = 3; 
				entry[2] = r;
				
				if (i < BUCKET_SIZE) {	
					if (low < i) {
						System.arraycopy(bucket, low, bucket, low + 1, i - low);			
						bucket[low] = entry;
					}
					else bucket[i] = entry;			
					freeSlot[bucketNo]++;
					continue;
				}
				
				/** Step 4 */
				Object[] oldBucket = bucket.clone();			
				if (gDepth > lDepths[bucketNo]) {
					lDepths[bucketNo]++;	
					bucket[freeSlot[bucketNo]++] = entry;
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
						buckets.add(null);
					
					//Rehash the key
					bucketNo = hash(k);
					tmp = buckets.get(bucketNo);
					
					if (tmp != null) {
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
					if (tmp != null) {
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
			return;
		}
		if(t == Types.getFloatType()) {
			float[] keys = (float[])colVals;
			float f;
			n = keys.length;
			for (r = 0; r < n; r++) {
				
				f = keys[r];
				
				if (f == Float.MAX_VALUE || f == Float.MIN_VALUE) 
					continue;
				
				k = Float.floatToIntBits(f);
				
				bucketNo = hash(k);
				tmp = buckets.get(bucketNo);
				i = freeSlot[bucketNo];

				added = false; 
				
				/** Step 1 */
				if (tmp == null) {
					entry = new int[INITIAL_CAPACITY];
					entry[0] = k;
					entry[1] = 3;
					entry[2] = r;
					bucket = new Object[BUCKET_SIZE];
					bucket[i++] = entry;
					freeSlot[bucketNo] = i;
					buckets.set(bucketNo, bucket);
					continue;
		 		}
				
				/** Step 2 */
				bucket = (Object[])tmp;
				int low = 0, high = i - 1;
				
				for (; low <= high;) {
					int mid = (low + high) >> 1;
					entry = (int[])bucket[mid];
					int tmpKey = entry[0];		//The beginning elements is key content
					if (tmpKey < k) 
						low = mid + 1;
					else if (tmpKey > k)
						high = mid - 1;
					// Key found, just insert the value into the matching data entry.
					else {				
						int entrySize = entry[1]; 	//The second elements are data entry size
						int low1 = 2, high1 = entrySize - 1;
						boolean valFound = false;
						for (; low1 <= high1;) {
							int mid1 = (low1 + high1) >> 1;
							int midVals = entry[mid1];
							if (midVals < r)
								low1 = mid1 + 1;
							else if (midVals > r)
								high1 = mid1 - 1;
							else {
								added = true;
								break;		//value found, do nothing			
							}
						}
						if (added)
							break;
						// Value not found, insert new value into current data entry.
						// First check for free slots in the entry. Expand entry if full
						if (entrySize == entry.length) {
							int[] newEntry = new int[Math.round(entrySize * FACTOR)];
							System.arraycopy(entry, 0, newEntry, 0, entrySize);
							entry = newEntry;
							bucket[mid] = entry;
						}
						// Then, insert value into current position
						if (low1 < entrySize) {
							System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
							entry[low1] = r;
						}
						else entry[entrySize] = r;
						entry[1]++;	
						added = true;
						break;
					}
				}
				if (added)
					continue;
				
				/** Step 3 */
				entry = new int[INITIAL_CAPACITY];
				entry[0] = k;
				entry[1] = 3; 
				entry[2] = r;
				
				if (i < BUCKET_SIZE) {	
					if (low < i) {
						System.arraycopy(bucket, low, bucket, low + 1, i - low);			
						bucket[low] = entry;
					}
					else bucket[i] = entry;			
					freeSlot[bucketNo]++;
					continue;
				}
				
				/** Step 4 */
				Object[] oldBucket = bucket.clone();			
				if (gDepth > lDepths[bucketNo]) {
					lDepths[bucketNo]++;	
					bucket[freeSlot[bucketNo]++] = entry;
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
						buckets.add(null);
					
					//Rehash the key
					bucketNo = hash(k);
					tmp = buckets.get(bucketNo);
					
					if (tmp != null) {
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
					if (tmp != null) {
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
			return;
		}
		if(t == Types.getLongType()) {
			long[] keys = (long[])colVals;
			n = keys.length;
			long l;
			for (r = 0; r < n; r++) {
				l = keys[r];
				
				if (l == Long.MAX_VALUE || l == Long.MIN_VALUE) 
					continue;
				
				k =  (int)(l ^ (l >>> 32));
				
				bucketNo = hash(k);
				tmp = buckets.get(bucketNo);
				i = freeSlot[bucketNo];

				added = false; 
				
				/** Step 1 */
				if (tmp == null) {
					entry = new int[INITIAL_CAPACITY];
					entry[0] = k;
					entry[1] = 3;
					entry[2] = r;
					bucket = new Object[BUCKET_SIZE];
					bucket[i++] = entry;
					freeSlot[bucketNo] = i;
					buckets.set(bucketNo, bucket);
					continue;
		 		}
				
				/** Step 2 */
				bucket = (Object[])tmp;
				int low = 0, high = i - 1;
				
				for (; low <= high;) {
					int mid = (low + high) >> 1;
					entry = (int[])bucket[mid];
					int tmpKey = entry[0];		//The beginning elements is key content
					if (tmpKey < k) 
						low = mid + 1;
					else if (tmpKey > k)
						high = mid - 1;
					// Key found, just insert the value into the matching data entry.
					else {				
						int entrySize = entry[1]; 	//The second elements are data entry size
						int low1 = 2, high1 = entrySize - 1;
						boolean valFound = false;
						for (; low1 <= high1;) {
							int mid1 = (low1 + high1) >> 1;
							int midVals = entry[mid1];
							if (midVals < r)
								low1 = mid1 + 1;
							else if (midVals > r)
								high1 = mid1 - 1;
							else {
								added = true;
								break;		//value found, do nothing			
							}
						}
						if (added)
							break;
						// Value not found, insert new value into current data entry.
						// First check for free slots in the entry. Expand entry if full
						if (entrySize == entry.length) {
							int[] newEntry = new int[Math.round(entrySize * FACTOR)];
							System.arraycopy(entry, 0, newEntry, 0, entrySize);
							entry = newEntry;
							bucket[mid] = entry;
						}
						// Then, insert value into current position
						if (low1 < entrySize) {
							System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
							entry[low1] = r;
						}
						else entry[entrySize] = r;
						entry[1]++;	
						added = true;
						break;
					}
				}
				if (added)
					continue;
				
				/** Step 3 */
				entry = new int[INITIAL_CAPACITY];
				entry[0] = k;
				entry[1] = 3; 
				entry[2] = r;
				
				if (i < BUCKET_SIZE) {	
					if (low < i) {
						System.arraycopy(bucket, low, bucket, low + 1, i - low);			
						bucket[low] = entry;
					}
					else bucket[i] = entry;			
					freeSlot[bucketNo]++;
					continue;
				}
				
				/** Step 4 */
				Object[] oldBucket = bucket.clone();			
				if (gDepth > lDepths[bucketNo]) {
					lDepths[bucketNo]++;	
					bucket[freeSlot[bucketNo]++] = entry;
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
						buckets.add(null);
					
					//Rehash the key
					bucketNo = hash(k);
					tmp = buckets.get(bucketNo);
					
					if (tmp != null) {
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
					if (tmp != null) {
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
			return;
		}
		Object[] keys = (Object[])colVals;
		n = keys.length;
		Object o;
		for (r = 0; r < n; r++) {
			o = keys[r];
			
			if (o == null || o == MyNull.NULLOBJ) 
				continue;
			
			k = o.hashCode();
			
			bucketNo = hash(k);
			tmp = buckets.get(bucketNo);
			i = freeSlot[bucketNo];

			added = false; 
			
			/** Step 1 */
			if (tmp == null) {
				entry = new int[INITIAL_CAPACITY];
				entry[0] = k;
				entry[1] = 3;
				entry[2] = r;
				bucket = new Object[BUCKET_SIZE];
				bucket[i++] = entry;
				freeSlot[bucketNo] = i;
				buckets.set(bucketNo, bucket);
				continue;
	 		}
			
			/** Step 2 */
			bucket = (Object[])tmp;
			int low = 0, high = i - 1;
			
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				entry = (int[])bucket[mid];
				int tmpKey = entry[0];		//The beginning elements is key content
				if (tmpKey < k) 
					low = mid + 1;
				else if (tmpKey > k)
					high = mid - 1;
				// Key found, just insert the value into the matching data entry.
				else {				
					int entrySize = entry[1]; 	//The second elements are data entry size
					int low1 = 2, high1 = entrySize - 1;
					boolean valFound = false;
					for (; low1 <= high1;) {
						int mid1 = (low1 + high1) >> 1;
						int midVals = entry[mid1];
						if (midVals < r)
							low1 = mid1 + 1;
						else if (midVals > r)
							high1 = mid1 - 1;
						else {
							added = true;
							break;		//value found, do nothing			
						}
					}
					if (added)
						break;
					// Value not found, insert new value into current data entry.
					// First check for free slots in the entry. Expand entry if full
					if (entrySize == entry.length) {
						int[] newEntry = new int[Math.round(entrySize * FACTOR)];
						System.arraycopy(entry, 0, newEntry, 0, entrySize);
						entry = newEntry;
						bucket[mid] = entry;
					}
					// Then, insert value into current position
					if (low1 < entrySize) {
						System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
						entry[low1] = r;
					}
					else entry[entrySize] = r;
					entry[1]++;	
					added = true;
					break;
				}
			}
			if (added)
				continue;
			
			/** Step 3 */
			entry = new int[INITIAL_CAPACITY];
			entry[0] = k;
			entry[1] = 3; 
			entry[2] = r;
			
			if (i < BUCKET_SIZE) {	
				if (low < i) {
					System.arraycopy(bucket, low, bucket, low + 1, i - low);			
					bucket[low] = entry;
				}
				else bucket[i] = entry;			
				freeSlot[bucketNo]++;
				continue;
			}
			
			/** Step 4 */
			Object[] oldBucket = bucket.clone();			
			if (gDepth > lDepths[bucketNo]) {
				lDepths[bucketNo]++;	
				bucket[freeSlot[bucketNo]++] = entry;
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
					buckets.add(null);
				
				//Rehash the key
				bucketNo = hash(k);
				tmp = buckets.get(bucketNo);
				
				if (tmp != null) {
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
				if (tmp != null) {
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
	public int[] pointQueryRowIDs(Object objKey) throws InvalidKeyException {
		int[] entry = new int[0];			
		int key = 0;
		
		//This is really stupid
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.getLength() == ((String)objKey).length())
				key = objKey.hashCode();
			else
				throw new InvalidKeyException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}						
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		
		if (tmp != null) { 		
			Object[] bucket = (Object[])tmp;
			int low = 0, high = freeSlot[bucketNo] - 1;
			int[] tmpEntry;				
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				tmpEntry = (int[])bucket[mid];
				int tmpKey = tmpEntry[0];		//The beginning elements is key content
				if (tmpKey < key) 
					low = mid + 1;
				else if (tmpKey > key)
					high = mid - 1;
				// Key found, just retrieve the values in entry from 3rd elements on
				else {					
					int size = tmpEntry[1];
					entry = new int[size - 2];
					System.arraycopy(tmpEntry, 2, entry, 0, size - 2);
					break;
				}
			}
		}
		return entry;
	}

	@Override
	public void delete(Object objKey, int rowID) throws InvalidKeyException {
		int[] entry;			
		int key = 0;
		
		//This is really stupid
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.getLength() == ((String)objKey).length())
				key = objKey.hashCode();
			else
				throw new InvalidKeyException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}						
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		int i = freeSlot[bucketNo];
		
		if (tmp != null) { 	
			Object[] bucket = (Object[])tmp;
			int low = 0, high = i - 1;
							
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				entry = (int[])bucket[mid];
				int tmpKey = entry[0];		//The beginning element is key content
				if (tmpKey < key) 
					low = mid + 1;
				else if (tmpKey > key)
					high = mid - 1;
				// Key found, further search for the matching row and delete if found.
				else {				
					boolean found = false;
					int entrySize = entry[1]; 	//The second element is data entry size
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
						System.arraycopy(entry, low1 + 1, entry, low1, entry.length - 1 - low1);
						entry[1]--;
						if (entry[1] == 0) {							
							System.arraycopy(bucket, mid + 1, bucket, mid, i - 1 - mid);
							freeSlot[bucketNo]--;
						}
					}
					return;
				}
			}	
		}
	}

	@Override
	public void update(Object objKey, int oldRowID, int newRowID) throws InvalidKeyException {
		int[] entry;			
		int key = 0;
		
		//This is really stupid
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.getLength() == ((String)objKey).length())
				key = objKey.hashCode();
			else
				throw new InvalidKeyException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}					
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		int i = freeSlot[bucketNo];
		
		if (tmp != null) { 	
			Object[] bucket = (Object[])tmp;
			int low = 0, high = i - 1;
							
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				entry = (int[])bucket[mid];
				int tmpKey = entry[0];		//The beginning element is key content
				if (tmpKey < key) 
					low = mid + 1;
				else if (tmpKey > key)
					high = mid - 1;
				// Key found, further search for the matching oldRow and delete
				else {				
					boolean found = false;
					int entrySize = entry[1]; 	//The second element is data entry size
					int low1 = 2, high1 = entrySize - 1;
					for (; low1 <= high1;) {
						int mid1 = (low1 + high1) >> 1;
						int midVals = entry[mid1];
						if (midVals < oldRowID)
							low1 = mid1 + 1;
						else if (midVals > oldRowID)
							high1 = mid1 - 1;
						else  {
							low1 = mid1;
							found = true;		//value found, stop searching
							break;				
						}
					}
					if (found) {
						System.arraycopy(entry, low1 + 1, entry, low1, entry.length - 1 - low1);
						entrySize = --entry[1];
						//insert a newRowID value
						low1 = 2; high1 = entrySize - 1;
						for (; low1 <= high1;) {
							int mid1 = (low1 + high1) >> 1;
							int midVals = entry[mid1];
							if (midVals < newRowID)
								low1 = mid1 + 1;
							else if (midVals > newRowID)
								high1 = mid1 - 1;
							else  return;		// new row found, dont need to insert
						}
						if (low1 < entrySize) {
							System.arraycopy(entry, low1, entry, low1 + 1, entrySize - low1);
							entry[low1] = newRowID;
						}
						else entry[entrySize] = newRowID;
						entry[1]++;				
					}
					return;
				}
			}	
		}
	}

	@Override
	public String describeIndex() {
		return des;
	}

	@Override
	public boolean supportRangeQueries() {
		return false;
	}

	//This method relies completely on the consistency of the index, it
	//assumes that every rows returned by index is not deleted in the
	//base table
	@Override
	public Operator<Row> pointQuery(Object objKey) throws InvalidKeyException {
		int[] entry = new int[0];			
		int key = 0;
		
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.equals(objKey))
				key = objKey.hashCode();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}				
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		
		if (tmp != null) { 		
			Object[] bucket = (Object[])tmp;
			int low = 0, high = freeSlot[bucketNo] - 1;
			int[] tmpEntry;				
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				tmpEntry = (int[])bucket[mid];
				int tmpKey = tmpEntry[0];		//The beginning elements is key content
				if (tmpKey < key) 
					low = mid + 1;
				else if (tmpKey > key)
					high = mid - 1;
				// Key found, just retrieve the values in entry from 3rd elements on
				else {					
					int size = tmpEntry[1];
					entry = new int[size - 2];
					System.arraycopy(tmpEntry, 2, entry, 0, size - 2);
					break;
				}
			}
		}
		return table.getRows(entry);		
	}

	@Override
	public void delete(Object objKey) throws InvalidKeyException {
		int[] entry;			
		int key = 0;
		
		//This is really stupid
		try {
			if (type == Types.getDateType()) 
				key = ((Date)objKey).hashCode();
			else if (type == Types.getDoubleType())
				key = ((Double)objKey).hashCode();
			else if (type == Types.getFloatType())
				key = ((Float)objKey).hashCode();
			else if (type == Types.getLongType())
				key = ((Long)objKey).hashCode();
			else if (type == Types.getIntegerType())
				key = ((Integer)objKey).hashCode();
			else if (type == Types.getVarcharType())
				key = ((String)objKey).hashCode();
			else if (type.getLength() == ((String)objKey).length())
				key = objKey.hashCode();
			else
				throw new InvalidKeyException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}						
		
		int bucketNo = hash(key);
		
		Object tmp = buckets.get(bucketNo);
		int i = freeSlot[bucketNo];
		
		if (tmp != null) { 	
			Object[] bucket = (Object[])tmp;
			int low = 0, high = i - 1;
							
			for (; low <= high;) {
				int mid = (low + high) >> 1;
				entry = (int[])bucket[mid];
				int tmpKey = entry[0];		//The beginning element is key content
				if (tmpKey < key) 
					low = mid + 1;
				else if (tmpKey > key)
					high = mid - 1;
				// Key found, further search for the matching row and delete if found.
				else {				
					System.arraycopy(bucket, mid + 1, bucket, mid, i - 1 - mid);
					freeSlot[bucketNo]--;
					return;
				}
			}	
		}
	}

	@Override
	public Table getBaseTable() {
		return table;
	}	
}