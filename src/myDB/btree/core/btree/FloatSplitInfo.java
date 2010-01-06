package myDB.btree.core.btree;


/**
 * Represents information from a split occurred at a lower level in the
 * tree.
 * 
 * @author jens
 */
public class FloatSplitInfo extends SplitInfo {
	
	FloatBTreeNode leftNode;

	float pivot;

	FloatBTreeNode rightNode;

	public FloatSplitInfo(FloatBTreeNode leftNode, float pivot, FloatBTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}