package myDB.btree.core.btree;

import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a node from the b-tree (either directory node or leaf).
 * 
 * @author jens/marcos
 */
public interface FloatBTreeNode extends BTreeNode {

	/**
	 * Obtains all values mapped to the given key. Values are delivered to the
	 * provided push operator.
	 * 
	 * @param key
	 * @param results
	 */
	public void get(float key, IntPushOperator results);

	/**
	 * Obtains all values mapped to the given key range (low and high,
	 * inclusive). Values are delivered to the provided push operator.
	 * 
	 * @param lowKey
	 * @param highKey
	 * @param results
	 */
	public void queryRange(float lowKey, float highKey, IntPushOperator results);

	/**
	 * Adds the given mapping to the node.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public FloatSplitInfo add(float key, int value, float lowKey, float highKey, LeafCarrier leafCarrier);

	/**
	 * Removes a single instance of the key-value mapping from the node. If the
	 * value given is equal to BTreeConstants.ALL_MAPPINGS then all mappings
	 * associated to the given key will be removed.
	 * 
	 * @param key
	 * @param lowKey
	 * @param highKey
	 */
	public void remove(float key, int value, float lowKey, float highKey);

	public void removeRange(float lowKey, float highKey);
	// do we need this method?
	// public boolean update(int key, int value);
}