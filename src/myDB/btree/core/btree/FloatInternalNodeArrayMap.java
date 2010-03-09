package myDB.btree.core.btree;

/**
 * Helper for b-tree nodes. This is a typed version similar to LeafArrayMap.
 * Generics not used due to inefficient use of complex types in Java 5.
 * 
 * @author jens
 */
public class FloatInternalNodeArrayMap extends InternalNodeArrayMap {

	/** n keys stored on this internal node */
	protected float[] keys;

	/**
	 * n + 1 pointers stored in this internal node - left is index-aligned with
	 * keys
	 */
	protected FloatBTreeNode[] nodes;

	public static int binarySearch(float[] a, float key, int from, int to) {
		int low = from;
		int high = to;

		for (; low <= high;) {
			int mid = (low + high) >> 1;
			float midVal = a[mid];

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found

		}
		return -(low + 1); // key not found.
	}

	public FloatInternalNodeArrayMap(int n) {
		keys = new float[n];
		nodes = new FloatBTreeNode[n + 1];

		nodes[0] = myDB.btree.core.btree.FloatInternalNode.NULL;
	}

	public float getMidKey() {
		return keys[currentSize / 2];
	}

	/**
	 * Splits this map, keeps entries from 0 to (mid-1) and returns a new map
	 * with entries from (mid+1) to (currentSize-1). The key mid is no floater
	 * present in either map and thus should be promoted.
	 * 
	 * @return
	 */
	public FloatInternalNodeArrayMap split() {
		FloatInternalNodeArrayMap newMap = new FloatInternalNodeArrayMap(
				keys.length);
		final int mid = currentSize / 2;
		int count = 0;
		newMap.nodes[0] = nodes[mid + 1];
		for (int i = mid + 1; i < currentSize; i++) {
			newMap.keys[count] = keys[i];
			newMap.nodes[++count] = nodes[i + 1];
		}

		// to allow garbage collection, nullify remaining pointers in original
		// map
		for (int i = mid; i < currentSize; i++) {
			nodes[i + 1] = null;
		}

		newMap.currentSize = currentSize - mid - 1;
		currentSize = mid;
		return newMap;
	}

	/**
	 * Puts the given key to rightNode association in the node array map.
	 * 
	 * @param key
	 * @param rightNode
	 * @return
	 */
	public void put(float key, FloatBTreeNode rightNode) {
		if (currentSize == 0) {
			keys[0] = key;
			nodes[1] = rightNode;
			currentSize++;
			return;
		}
		int pos = binarySearch(keys, key, 0, currentSize - 1);
		if (pos >= 0) { // key exists, replace:
			keys[pos] = key;
			nodes[pos + 1] = rightNode;
		} else { // key does not exist, insert:
			pos = -(pos + 1);

			if (pos < currentSize) {
				System.arraycopy(keys, pos, keys, pos + 1, currentSize - pos);
				System.arraycopy(nodes, pos + 1, nodes, pos + 2, currentSize
						- pos);
				keys[pos] = key;
				nodes[pos + 1] = rightNode;
				currentSize++;
			} else {
				keys[currentSize] = key;
				nodes[currentSize + 1] = rightNode;
				currentSize++;
			}
		}
	}

	/**
	 * Returns the node corresponding to the interval in which the provided key
	 * falls.
	 * 
	 * @param key
	 * @return
	 */
	public FloatBTreeNode get(float key) {
		int pos = getIntervalPosition(key);
		if (pos == -1)
			return null;
		else
			return nodes[pos];
	}

	/**
	 * Obtains the position in the nodes array that represents the interval in
	 * which the provided key falls.
	 * 
	 * @param key
	 * @return
	 */
	public int getIntervalPosition(float key) {
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
	 * Returns false if key was not found. This method does not touch the
	 * left-most node in the array map, as the left-most node property of having
	 * keys smaller than the key of the left-most key will be kept if that key
	 * is deleted.
	 * 
	 * @param key
	 * @return
	 */
	public boolean delete(float key) {
		if (currentSize == 0) {
			return false;
		}
		int pos = binarySearch(keys, key, 0, currentSize - 1);
		if (pos >= 0) { // key exists, delete:
			deleteAtPos(pos);
			return true;
		} else { // key does not exist, return false:
			return false;
		}
	}

	/**
	 * Deletes the key-node mapping at the given position.
	 * 
	 * @param pos
	 */
	@Override
	public void deleteAtPos(int pos) {
		System.arraycopy(keys, pos + 1, keys, pos, currentSize - pos);
		System.arraycopy(nodes, pos + 2, nodes, pos + 1, currentSize - pos);
		nodes[currentSize] = null; // allow garbage-collection
		currentSize--;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		if (nodes[0] == myDB.btree.core.btree.FloatInternalNode.NULL) {
			sb.append("NULL | ");
		} else {
			String nodeValue = nodes[0] == null ? null : Integer
					.toString(nodes[0].hashCode());
			sb.append(nodeValue + " | ");
		}
		for (int i = 0; i < currentSize; i++) {
			sb.append(keys[i] + " | ");

			String nodeValue = nodes[i + 1] == null ? null : Integer
					.toString(nodes[i + 1].hashCode());
			sb.append(nodeValue);
			if (i + 1 < currentSize)
				sb.append(" | ");

		}

		return sb.toString();
	}

	@Override
	public int size() {
		return currentSize;
	}

}
