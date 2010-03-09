package myDB.btree.core.btree;

/**
 * Represents information from a split occurred at a lower level in the tree.
 * 
 * @author jens
 */
public class ObjectSplitInfo extends SplitInfo {

	ObjectBTreeNode leftNode;

	Comparable pivot;

	ObjectBTreeNode rightNode;

	public ObjectSplitInfo(ObjectBTreeNode leftNode, Comparable pivot,
			ObjectBTreeNode rightNode) {
		this.leftNode = leftNode;
		this.pivot = pivot;
		this.rightNode = rightNode;
	}
}