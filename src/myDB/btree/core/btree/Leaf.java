package myDB.btree.core.btree;

/**
 * Represents a b-tree leaf node.
 * 
 * @author jens/marcos
 */
public abstract class Leaf implements BTreeNode {
	//my addition
	//int type;	

	protected int k_star;


	public boolean isLeaf() {
		return true;
	}
}