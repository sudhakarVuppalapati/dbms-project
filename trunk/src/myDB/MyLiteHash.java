package myDB;

public class MyLiteHash {

	int[][] buckets; 
	
	public MyLiteHash() {
		buckets = new int[100][];
	}
	
	public int[] findBucket(int key) {
		int[] bucket = buckets[hash32shiftmult(key) % 100];
		if (bucket == null) return new int[0];
		else return bucket;
	}
	
	public boolean add(int key, int rowID) {
		int hashCode = hash32shiftmult(key) % 100;
		int[] tmp;
		if (buckets[hashCode] == null) {
			buckets[hashCode] = new int[40];
			buckets[hashCode][0] = 1;
			buckets[hashCode][1] = rowID;
			return true;
		}
		else {
			int n = buckets[hashCode].length;
			//Extend the bucket if needed
			int pos = binarySearch(buckets[hashCode], rowID, n);
			if (pos < 0) {
				if (buckets[hashCode][0] == n) {
					tmp = new int[n * 3 / 2 + 1];
					System.arraycopy(buckets[hashCode], 0, tmp, 0, n);
					buckets[hashCode] = tmp;
					buckets[hashCode][n] = rowID;
					buckets[hashCode][0]++;
				}
				pos = -(pos + 1);
				System.arraycopy(buckets[hashCode], pos, buckets[hashCode], pos + 1, n - pos);
				buckets[hashCode][pos] = rowID;
				return true;
			}
			else return false;
		}
	}
	
	public boolean delete(int key, int rowID) {
		int hashCode = hash32shiftmult(key) % 100;
		if (buckets[hashCode] == null) return false;
		else {
			int n = buckets[hashCode].length;
			int pos = binarySearch(buckets[hashCode], rowID, n);
			if (pos < 0) return false;
			else {
				System.arraycopy(buckets[hashCode], pos + 1, buckets[hashCode], pos, n - pos - 1);
				buckets[hashCode][0]--;
				return true;
			}
		}
	}
	
	/** Return the first occurrence of matching (or greater) rowID, 
	 * ignoring the first element */
	private static final int binarySearch(int[] arr, int rowID, int length) {
		int low = 1, high = length - 1;
		for (; low <= high;) {
			int mid = (low + high) >> 1;			
			if (rowID < arr[mid])
				high  = mid - 1;
			else if (rowID > arr[mid])
				low = mid + 1;
			else return mid;
		}
		return -(low + 1);
	}	
	
	public static void main(String[] atrgs) {
		System.out.println(binarySearch(new int[] {4, 1, 5, 9, 12}, 9, 5));
	}

	/**
	 * Based on Robert Jenkins' bit Mix Function, version in Java developed by
	 * Thomas Wang (http://www.concentric.net/~Ttwang/tech/inthash.htm)
	 * Copyright (c) 2007 January
	 * 
	 * @param key
	 *            the input key, as 32-bit integer
	 * @return the hash value
	 */
	private static final int hash32shiftmult(int key) {
		int c2 = 0x27d4eb2d; // a prime or an odd constant
		key = (key ^ 61) ^ (key >>> 16);
		key = key + (key << 3);
		key = key ^ (key >>> 4);
		key = key * c2;
		key = key ^ (key >>> 16);
		return key;
	}
}
