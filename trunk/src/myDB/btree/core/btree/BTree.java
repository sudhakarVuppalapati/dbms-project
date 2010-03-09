package myDB.btree.core.btree;

import java.io.OutputStream;

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
public abstract class BTree {

	/**
	 * The first time constructing internal node from leaf node. Need to be
	 * careful to avoid losing overflow leaves
	 */
	protected boolean firstTime = true;

	/** the degree of the b-tree (internal nodes) */
	protected int k;

	/** the degree of the leaves */
	protected int k_star;

	protected double leafUtilization;

	protected int leafCount;

	protected int elemCount;

	protected boolean refreshNeeded = true;

	/**
	 * Instantiates a new BTree.
	 * 
	 * @param k
	 * @param k_star
	 */
	public BTree(int k, int k_star) {
		this.k = k;
		this.k_star = k_star;
	}

	/**
	 * generates a dotty representation of the tree in the given output stream.
	 */
	public abstract void toDot(OutputStream dest);

	public void printStats() {
		calculateStats();
		System.out.print("leafUtilization:\t" + leafUtilization
				+ "\tleafCount:\t" + leafCount + "\telementCount:\t"
				+ elemCount + "\t");
	}

	public long size() {
		calculateStats();
		return elemCount;
	}

	protected abstract void calculateStats();

	public double getLeafUtilization() {
		calculateStats();
		return leafUtilization;
	}

	public int getLeafCount() {
		calculateStats();
		return leafCount;
	}

	public int getElemCount() {
		calculateStats();
		return elemCount;
	}

}
