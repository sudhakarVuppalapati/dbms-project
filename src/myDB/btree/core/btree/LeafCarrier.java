package myDB.btree.core.btree;

/**
 * This class should be used to return the leaf in which an add operation inserted the mapping given to the b-tree.
 * 
 * @author marcos
 *
 */
public class LeafCarrier {

	/**
	 * The leaf to be returned.
	 */
	protected Leaf carriedLeaf;
	
	public Leaf getLeaf() {
		return carriedLeaf;
	}
	
}
