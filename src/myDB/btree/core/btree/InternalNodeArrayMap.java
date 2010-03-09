package myDB.btree.core.btree;

/**
 * Helper for b-tree nodes. This is a typed version similar to LeafArrayMap.
 * Generics not used due to inefficient use of complex types in Java 5.
 * 
 * @author jens
 */
public abstract class InternalNodeArrayMap {

	/**
	 * number of keys in this map. Note that we have one extra pointer to the
	 * left (position 0).
	 */
	protected int currentSize = 0;

	/**
	 * Deletes the key-node mapping at the given position.
	 * 
	 * @param pos
	 */
	public abstract void deleteAtPos(int pos);

	public int size() {
		return currentSize;
	}

}
