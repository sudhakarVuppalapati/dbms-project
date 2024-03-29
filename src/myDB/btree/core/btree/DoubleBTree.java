package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * A simple btree implementation. (key equal or less than pivot -> go right) Has
 * to be taylored to different key and value types manually as Generics would
 * use Complex type (inefficent) instead of native types. We have pointers among
 * leaves and also only store key/value mappings on the leaves. Therefore, this
 * is a B+-tree implementation.
 * <p>
 * The implementation allows us to store duplicate keys in the leaf level of the
 * tree. However, a strict tree interval invariant is kept. To handle duplicate
 * keys, this means that overflows due to duplication are handled by adding
 * special overflow leaves that are not pointed to by any parent node. These
 * leaves are created via splitting, however no pivot is promoted on such a
 * split.
 * <p>
 * The delete strategy implemented in this tree is to just remove and not merge.
 * Thus, there is no logic to merge at nodes at half occupation and nodes may
 * become underutilized. This may be monitored by calculating the utilization at
 * the leaf level.
 * 
 * @author jens / marcos
 */
public class DoubleBTree extends BTree {

	/** the root of the b-tree */
	protected DoubleBTreeNode root = null;

	/** the left-most leaf on the b-tree */
	protected DoubleLeaf firstLeaf = null;

	/**
	 * Instantiates a new BTree.
	 * 
	 * @param k
	 * @param k_star
	 */
	public DoubleBTree(int k, int k_star) {
		super(k, k_star);
	}

	/**
	 * Adds a mapping from key to value in the b-tree. Duplicate mappings are
	 * allowed.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public void add(double key, int value, LeafCarrier leafCarrier) {
		if (root == null) {
			firstLeaf = new DoubleLeaf(k_star);
			root = firstLeaf;
		}
		DoubleSplitInfo splitInfo = root.add(key, value, Double.MIN_VALUE,
				Double.MAX_VALUE, leafCarrier);
		if (splitInfo != null) {
			DoubleInternalNode newRoot;
			// root overflow!:
			if (firstTime) {
				newRoot = new DoubleInternalNode(root, splitInfo.pivot,
						splitInfo.rightNode, k);
				firstTime = false;
			} else
				newRoot = new DoubleInternalNode(splitInfo.leftNode,
						splitInfo.pivot, splitInfo.rightNode, k);

			root = newRoot;
		}
		refreshNeeded = true;
	}

	public void add(double key, int value) {
		add(key, value, null);
	}

	/**
	 * Gets the value currently mapped to the given key.
	 * 
	 * @param key
	 * @return
	 */
	public void get(double key, IntPushOperator results) {
		root.get(key, results);
	}

	/**
	 * Removes all mappings corresponding to the given key from the b-tree.
	 * 
	 * @param key
	 * @return
	 */
	public void remove(double key) {
		root.remove(key, BTreeConstants.ALL_MAPPINGS, Double.MIN_VALUE,
				Double.MAX_VALUE);
		refreshNeeded = true;
	}

	/**
	 * Removes one instance of the given key-value mapping from the b-tree. Note
	 * that even if multiple instances of that mapping exist, only a single
	 * instance will be removed.
	 * 
	 * @param key
	 * @param value
	 */
	public void remove(double key, int value) {
		root.remove(key, value, Double.MIN_VALUE, Double.MAX_VALUE);
		refreshNeeded = true;
	}

	public void remove(double lowKey, double highKey) {
		root.removeRange(lowKey, highKey);
		refreshNeeded = true;
	}

	/**
	 * Returns all the values mapped in the given key range through the provided
	 * push operator. We include values that correspond to the lowKey and also
	 * include values that correspond to the highKey.
	 * 
	 * @param lowKey
	 * @param highKey
	 * @return
	 */
	public void queryRange(double lowKey, double highKey,
			IntPushOperator results) {
		root.queryRange(lowKey, highKey, results);
	}

	/**
	 * Prints the root of the tree as a string.
	 */
	@Override
	public String toString() {
		return root.toString();
	}

	/**
	 * generates a dotty representation of the tree in the given output stream.
	 */
	@Override
	public void toDot(OutputStream dest) {
		try {
			// write header
			dest.write("digraph g {\n".getBytes());
			dest.write("node [shape=record,height=.1];\n".getBytes());

			// write tree internal nodes
			root.toDot(dest);

			// write tree leaf nodes
			DoubleLeaf currentLeaf = firstLeaf;
			while (currentLeaf != null) {
				currentLeaf.toDot(dest);
				currentLeaf = currentLeaf.nextLeaf;
			}

			// write trailer, flush, and close
			dest.write("}\n".getBytes());
			dest.flush();
			dest.close();
		} catch (IOException e) {
			System.out.println("could not write dotty");
			e.printStackTrace();
		}
	}

	@Override
	protected void calculateStats() {
		if (refreshNeeded) {
			DoubleLeaf currentLeaf = firstLeaf;
			elemCount = 0;
			leafCount = 0;
			while (currentLeaf != null) {
				leafCount++;
				elemCount += currentLeaf.entries.size();

				currentLeaf = currentLeaf.nextLeaf;
			}

			leafUtilization = (double) elemCount
					/ (double) (leafCount * 2 * k_star);
			refreshNeeded = false;
		}
	}
}
