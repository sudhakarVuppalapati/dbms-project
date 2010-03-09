package myDB.btree.core.btree;

/**
 * Represents information from a split occurred at a lower level in the tree.
 * 
 * @author jens
 */
public class DoubleSplitInfo extends SplitInfo {

	DoubleBTreeNode leftNode;

	double pivot;

	DoubleBTreeNode rightNode;

	public DoubleSplitInfo(DoubleBTreeNode leftNode, double pivot,
			DoubleBTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}