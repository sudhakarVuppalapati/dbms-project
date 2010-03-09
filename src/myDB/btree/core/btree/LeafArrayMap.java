package myDB.btree.core.btree;

/**
 * Helper for b-tree leaves. Has to be taylored to different key and value types
 * manually as Generics would use Complex type (inefficent) instead of native
 * types.
 * 
 * @author jens
 */
public abstract class LeafArrayMap {

	// protected int[] keys;

	protected int[] values;

	protected int currentSize = 0;

	protected static final int STOP = 0;

	protected static final int CONTINUE_WITH_BINSEARCH = 1;

	protected static final int CONTINUE_WITH_SCAN = 2;

	public int size() {
		return currentSize;
	}
}
