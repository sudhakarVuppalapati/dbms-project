package myDB.btree.core.btree;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a node from the b-tree (either directory node or leaf).
 * 
 * @author jens/marcos
 */
public interface DoubleBTreeNode extends BTreeNode {

	/**
	 * Obtains all values mapped to the given key. Values are delivered to the
	 * provided push operator.
	 * 
	 * @param key
	 * @param results
	 */
	public void get(double key, IntPushOperator results);

	/**
	 * Obtains all values mapped to the given key range (low and high,
	 * inclusive). Values are delivered to the provided push operator.
	 * 
	 * @param lowKey
	 * @param highKey
	 * @param results
	 */
	public void queryRange(double lowKey, double highKey,
			IntPushOperator results);

	/**
	 * Adds the given mapping to the node.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public DoubleSplitInfo add(double key, int value, double lowKey,
			double highKey, LeafCarrier leafCarrier);

	/**
	 * Removes a single instance of the key-value mapping from the node. If the
	 * value given is equal to BTreeConstants.ALL_MAPPINGS then all mappings
	 * associated to the given key will be removed.
	 * 
	 * @param key
	 * @param lowKey
	 * @param highKey
	 */
	public void remove(double key, int value, double lowKey, double highKey);

	public void removeRange(double lowKey, double highKey);
	// do we need this method?
	// public boolean update(int key, int value);
}