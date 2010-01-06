package myDB.btree.core.btree;


/**
 * Represents information from a split occurred at a lower level in the
 * tree.
 * 
 * @author jens
 */
public class IntSplitInfo extends SplitInfo {
	
	IntBTreeNode leftNode;

	int pivot;

	IntBTreeNode rightNode;

	public IntSplitInfo(IntBTreeNode leftNode, int pivot, IntBTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}