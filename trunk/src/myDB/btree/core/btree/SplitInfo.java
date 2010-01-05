package myDB.btree.core.btree;


/**
 * Represents information from a split occurred at a lower level in the
 * tree.
 * 
 * @author jens
 */
public class SplitInfo {
	BTreeNode leftNode;

	int pivot;

	BTreeNode rightNode;

	public SplitInfo(BTreeNode leftNode, int pivot, BTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}