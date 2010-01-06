package myDB.btree.core.btree;

import java.io.OutputStream;

/**
 * Represents a node from the b-tree (either directory node or leaf).
 * 
 * @author jens/marcos
 */
public interface BTreeNode {

	/**
	 * Indicates if this node is a leaf.
	 * 
	 * @return
	 */
	public boolean isLeaf();

	/**
	 * Indicates if this node is empty.
	 * 
	 * @return
	 */
	public boolean isEmpty();

	/**
	 * Serializes this node to a dotty representation (to be rendered with
	 * graphviz).
	 * 
	 * @param dest
	 */
	public void toDot(OutputStream dest);

	// do we need this method?
	// public boolean update(int key, int value);
}