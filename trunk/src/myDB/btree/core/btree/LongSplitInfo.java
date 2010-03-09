package myDB.btree.core.btree;

/**
 * Represents information from a split occurred at a lower level in the tree.
 * 
 * @author jens
 */
public class LongSplitInfo extends SplitInfo {

	LongBTreeNode leftNode;

	long pivot;

	LongBTreeNode rightNode;

	public LongSplitInfo(LongBTreeNode leftNode, long pivot,
			LongBTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}