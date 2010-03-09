package myDB.btree.core.btree;

import myDB.btree.util.IntPushOperator;


/**
 * Helper for b-tree leaves. Has to be taylored to different key and value types
 * manually as Generics would use Complex type (inefficent) instead of native
 * types.
 */
public class ObjectLeafArrayMap extends LeafArrayMap {

	protected Comparable[] keys;

	protected int[] values;

	public int getIntervalPosition(Comparable key) {
		if (currentSize == 0) {
			return -1;
		} else {
			int pos = binarySearch(keys, key, 0, currentSize - 1);

			// we are left-aligned, so we take equal to the right, non-equal at
			// insertion point
			if (pos < 0) {
				// key not found: calculate insertion point
				pos = -(pos + 1);
			} else {
				// key found: take right path
				pos++;
			}
			return pos;
		}
	}
	/**
	 * This binary search method is modified to guarantee that, in the presence
	 * of duplicate keys, we will always return the first occurrence of a found
	 * key in the portion of the array being searched (from-to).
	 * 
	 * @param a
	 * @param key
	 * @param from
	 * @param to
	 * @return
	 */
	public static int binarySearch(Comparable[] a, Comparable key, int from, int to) {
		int low = from;
		int high = to;

		for (; low <= high;) {
			int mid = (low + high) >> 1;
			Comparable midVal = a[mid];

			if (key.compareTo(midVal) > 0)
				low = mid + 1;
			else if (key.compareTo(midVal) < 0)
				high = mid - 1;
			else {
				// key found: search for first occurrence linearly
				// this search is necessary in the presence of duplicates
				int pos = mid - 1;
				while (pos >= from && a[pos].equals(key)) {
					pos--;
				}
				// return last valid position
				return pos + 1;
			}

		}
		return -(low + 1); // key not found.
	}

	public ObjectLeafArrayMap(int n) {
		keys = new Comparable[n];
		values = new int[n];
	}

	public Comparable getMidKey() {
		return keys[currentSize / 2];
	}

	/**
	 * Splits this map. The split operation will attempt to split this map in
	 * the middle key.
	 * 
	 * @return
	 */
	public ObjectLeafArrayMap split() {
		ObjectLeafArrayMap newMap = new ObjectLeafArrayMap(keys.length);
		final int mid = currentSize / 2;
		int count = 0;
		for (int i = mid; i < currentSize; i++) {
			newMap.keys[count] = keys[i];
			newMap.values[count] = values[i];
			count++;
		}

		newMap.currentSize = currentSize - mid;
		currentSize = mid;
		return newMap;
	}

	/**
	 * Associates the given key with the given value in this array map, if the
	 * insertion point for the key may be found here. If that is not the case,
	 * the method will return true, indicating that the search for the insertion
	 * point should be continued on the next array.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean tryAdd(Comparable key, int value) {
		if (currentSize == 0) {
			// insert in this node: we assume overflow leaves are
			// garbage-collected on deletions
			keys[0] = key;
			values[0] = value;
			currentSize++;
			return false;
		} else {
			int pos = binarySearch(keys, key, 0, currentSize - 1);
			if (pos < 0) {
				// calculate insertion point
				pos = -(pos + 1);
			}

			// find insertion point
			while (pos < currentSize && key.compareTo(keys[pos]) >= 0 ) {
				pos++;
			}
			if (pos == currentSize) {
				// continue search for insertion point in next leaf
				return true;
			} else {
				// well, the insertion point is here
				addAtPos(key, value, pos);
				return false;
			}
		}
	}

	/**
	 * Adds the given mapping at the given position in this array.
	 * 
	 * @param key
	 * @param value
	 * @param pos
	 */
	public void addAtPos(Comparable key, int value, int pos) {
		if (pos < currentSize) {
			System.arraycopy(keys, pos, keys, pos + 1, currentSize - pos);
			System.arraycopy(values, pos, values, pos + 1, currentSize - pos);
			currentSize++;

			keys[pos] = key;
			values[pos] = value;
		} else {
			keys[currentSize] = key;
			values[currentSize] = value;
			currentSize++;
		}
	}

	/**
	 * Gets the values for the specified key and pushes them in the given push
	 * operator. If we have scanned until the last position of this array, then
	 * maybe more values can be found in the next array. This method returns
	 * true in this situation.
	 */
	public int get(Comparable key, IntPushOperator results) {
		if (currentSize == 0) {
			// continue search
			return CONTINUE_WITH_BINSEARCH;
		} else {
			int pos = binarySearch(keys, key, 0, currentSize - 1);
			if (pos < 0) {
				// key not found: if we are at the end of the array, maybe key
				// is in the next one
				pos = -(pos + 1);
				return pos == currentSize ? CONTINUE_WITH_BINSEARCH : STOP;
			} else {
				// get values corresponding to key
				return continueGet(pos, key, results);
			}
		}
	}

	public int continueGet(int pos, Comparable key, IntPushOperator results) {
		while (pos < currentSize && key.compareTo(keys[pos]) == 0) {
			results.pass(values[pos]);
			pos++;
		}
		return pos == currentSize ? CONTINUE_WITH_SCAN : STOP;
	}

	/**
	 * Remove mappings with the given key. If a value is given, then only a
	 * single mapping is removed. If BTreeConstants.ALL_MAPPINGS is given, then
	 * all mappings for the given key are removed. Returns true if removal must
	 * continue searching on the next leaf.
	 * 
	 * @param key
	 * @return
	 */
	public boolean remove(Comparable key, int value) {
		if (currentSize == 0) {
			// continue search
			return true;
		}
		int pos = binarySearch(keys, key, 0, currentSize - 1);
		if (pos < 0) {
			// key does not exist here, check if we should go to next array
			pos = -(pos + 1);
			return pos == currentSize;
		} else {
			// key exists, delete:
			// first find occurrence range of key in this array
			int firstOccurrence = -1; // pos
			int lastOccurrence;

			while (pos < currentSize && key.compareTo(keys[pos]) == 0) {

				if (value == BTreeConstants.ALL_MAPPINGS) {
					// mark first occurrence
					if (firstOccurrence == -1) {
						firstOccurrence = pos;
					}
				} else {
					if (values[pos] == value) {
						// found desired mapping: only that mapping should be
						// removed
						firstOccurrence = pos;
						break;
					}
				}
				// continue scanning
				pos++;
			}
			// fix last occurrence
			if (value == BTreeConstants.ALL_MAPPINGS) {
				lastOccurrence = pos - 1;
			} else {
				lastOccurrence = firstOccurrence;
			}
			boolean continueSearch = (value == BTreeConstants.ALL_MAPPINGS)
					|| (firstOccurrence == -1) ? (pos == currentSize) : false;

			// now delete all occurrences in one move, if necessary
			/** TENTATIVE -$BEGIN */
					
			if (firstOccurrence != -1) {
				System.arraycopy(keys, lastOccurrence + 1, keys,
						firstOccurrence, currentSize - (lastOccurrence + 1));
				System.arraycopy(values, lastOccurrence + 1, values,
						firstOccurrence, currentSize - (lastOccurrence + 1));
				currentSize -= (lastOccurrence - firstOccurrence + 1);
			}
					
			/*if (firstOccurrence != -1) {
				if (lastOccurrence + 1 != currentSize) {
					System.arraycopy(keys, lastOccurrence + 1, keys,
							firstOccurrence, currentSize - (lastOccurrence + 1));
					System.arraycopy(values, lastOccurrence + 1, values,
							firstOccurrence, currentSize - (lastOccurrence + 1));
					currentSize -= (lastOccurrence - firstOccurrence + 1);
					
				}
				else {
					currentSize = firstOccurrence;
				}
			}*/
			
			/** TENTATIVE -$END */

			return continueSearch;
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < currentSize; i++) {
			sb.append(keys[i] + "=");
			sb.append(values[i]);
			if (i + 1 < currentSize)
				sb.append(", ");

		}

		return sb.toString();
	}

	/**
	 * Obtains all values mapped to the given key range in this array. If the
	 * search stops in the last position of the array, then this method returns
	 * true, flagging that continuing to the next array is necessary.
	 * 
	 * @param lowKey
	 * @param highKey
	 * @param results
	 * @return
	 */
	public int queryRange(Comparable lowKey, Comparable highKey, IntPushOperator results) {
		if (currentSize == 0) {
			return CONTINUE_WITH_BINSEARCH; // maybe leaf was emptied by
			// deletions
		} else {
			int pos = binarySearch(keys, lowKey, 0, currentSize - 1);
			if (pos < 0) {
				// key not found: get starting point for scan
				pos = -(pos + 1);
			}

			// scan from given position onwards
			return continueScan(pos, highKey, results);
		}
	}

	public int continueScan(int pos, Comparable highKey, IntPushOperator results) {
		boolean returnedSomething = false;
		while (pos < currentSize && highKey.compareTo(keys[pos]) >= 0) {
			results.pass(values[pos]);
			pos++;
			returnedSomething = true;
		}
		return pos == currentSize ? (returnedSomething ? CONTINUE_WITH_SCAN
				: CONTINUE_WITH_BINSEARCH) : STOP;
	}

	public void deleteAtPos(int i) {
		System.arraycopy(keys, i + 1, keys, i, currentSize - (i + 1));
		System.arraycopy(values, i + 1, values, i, currentSize - (i + 1));
		currentSize--;
	}

}
