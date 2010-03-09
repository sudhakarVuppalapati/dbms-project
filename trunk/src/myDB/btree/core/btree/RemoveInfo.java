package myDB.btree.core.btree;

/**
 * Stores information about a deletion occurred at a lower level of the b-tree.
 * 
 * @author marcos
 */
public class RemoveInfo {

	int removedValue;

	boolean redistribution;

	int pivot; // for redistributions

	boolean merge;

	boolean operationWithNext;

	public RemoveInfo(int removedValue, boolean redistribution, int pivot,
			boolean merge, boolean operationWithNext) {
		this.removedValue = removedValue;
		this.redistribution = redistribution;
		this.pivot = pivot;
		this.merge = merge;
		this.operationWithNext = operationWithNext;
	}

}
