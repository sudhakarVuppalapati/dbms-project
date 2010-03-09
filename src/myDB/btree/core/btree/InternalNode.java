package myDB.btree.core.btree;

import java.io.OutputStream;

/**
 * Represents a b-tree directory node.
 * 
 * @author jens/marcos
 */

public class InternalNode implements BTreeNode {

	protected int k;

	// protected InternalNode nextNode;

	public InternalNode(int k) {
		this.k = k;
	}

	public void toDot(OutputStream dest) {
	}

	public boolean isLeaf() {
		return false;
	}

	public boolean isEmpty() {
		return false;
	}

}