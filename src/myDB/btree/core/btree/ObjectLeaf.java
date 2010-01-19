package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree leaf node.
 * 
 * @author jens/marcos
 */
public class ObjectLeaf extends Leaf implements ObjectBTreeNode {
	//my addition
	//int type;	
	protected ObjectLeafArrayMap entries;

	//protected int k_star;

	protected ObjectLeaf nextLeaf;

	public ObjectLeaf(int k_star, ObjectLeafArrayMap entries) {
		this.k_star = k_star;
		this.entries = entries;
		this.nextLeaf = null;
	}

	public ObjectLeaf(int k_star) {
		this(k_star, new ObjectLeafArrayMap(2 * k_star));
	}

	public ObjectSplitInfo add(Comparable key, int value, Comparable lowKey, Comparable highKey,
			LeafCarrier leafCarrier) {		
		// search for insertion point: last allowed node of a scan for that key
		// a node is allowed as far as its first key is smaller than the highKey
		// this keeps the strict tree invariant that a leaf pointed to by
		// a given node only contains keys in the interval [lowKey, highKey).

		// HACK: even if a node is empty, we know that its array still contains
		// the key values it used to have when it was full. That allows us to
		// obtain the key range for a next leaf even if its entries are
		// completely deleted. This is necessary because we know that nodes
		// never get merged.
		boolean continueSearch = entries.tryAdd(key, value);
		ObjectLeaf currentLeaf = this;
		while (continueSearch && currentLeaf.nextLeaf != null
				&& highKey.compareTo(currentLeaf.nextLeaf.entries.keys[0]) > 0) {
			continueSearch = currentLeaf.nextLeaf.entries.tryAdd(key, value);
			currentLeaf = currentLeaf.nextLeaf;
		}
		if (continueSearch) {
			// key was not added anywhere, so add it to this leaf anyway (no
			// more next leaves are available)
			currentLeaf.entries
					.addAtPos(key, value, currentLeaf.entries.size());
		}

		// set leaf carrier, as appropriate
		if (leafCarrier != null) {
			leafCarrier.carriedLeaf = currentLeaf;
		}

		// invariant: key was added to currentLeaf
		if (currentLeaf.entries.size() >= 2 * k_star) {
			// split this leaf
			return currentLeaf.split();
		}
		return null;
	}

	/**
	 * Splits this leaf and returns the split information if the split needs to
	 * be propagated.
	 * 
	 * Procedure is:
	 * <p>
	 * Try to get pivot as middle key. However, we must guarantee that this key
	 * does not appear already as the last key in the previous leaf.
	 * <p>
	 * If it does, then we search for the next key that does not.
	 * <p>
	 * If such key does not exist, then there is no pivot, and the split is not
	 * propagated (that is, the upper levels are already correct).
	 * 
	 * @param previousLeaf
	 * @return
	 */
	private ObjectSplitInfo split() {
		// split leaf into two new nodes:
		ObjectLeaf newLeaf = new ObjectLeaf(k_star, entries.split());

		// make sure we point to this new sibling
		newLeaf.nextLeaf = nextLeaf;
		nextLeaf = newLeaf;

		// pivot is first entry in newLeaf keys
		if (newLeaf.entries.keys[0].compareTo(entries.keys[entries.size() - 1]) == 0) {
			// do not propagate split: pivot does not differentiate leaves
			// this means that the new leaf is an overflow leaf and has no
			// parent node
			return null;
		} else {
			// propagate split: pivot differentiates the two leaves, so the new
			// leaf does not have to be an overflow leaf
			return new ObjectSplitInfo(this, newLeaf.entries.keys[0], newLeaf);
		}
	}

	public void remove(Comparable key, int value, Comparable lowKey, Comparable highKey) {
		// continue search on the leaf level until all entries with the given
		// key are guaranteed to be removed
		boolean continueSearch = entries.remove(key, value);
		ObjectLeaf currentLeaf = this;

		// HACK: even if a node is empty, we know that its array still contains
		// the key values it used to have when it was full. That allows us to
		// obtain the key range for a next leaf even if its entries are
		// completely deleted. This is necessary because we know that nodes
		// never get merged. (same as in insertion)
		// we use this information below to test if the nextLeaf is still in the
		// allowed range for this deletion
		while (continueSearch && currentLeaf.nextLeaf != null
				&& highKey.compareTo(currentLeaf.nextLeaf.entries.keys[0]) > 0) {
			continueSearch = currentLeaf.nextLeaf.entries.remove(key, value);

			// check if we should garbage-collect currentLeaf.nextLeaf
			// that happens when it is an empty leaf not pointed to by a parent
			// this is true of all leaves visited in this loop
			if (currentLeaf.nextLeaf.entries.size() == 0) {
				currentLeaf.nextLeaf = currentLeaf.nextLeaf.nextLeaf;
			} else {
				// move on, there may be more stuff to remove
				currentLeaf = currentLeaf.nextLeaf;
			}
		}
	}

	public void removeRange1(Comparable lowKey, Comparable highKey) {
		int currentSize = entries.currentSize;
		int pos = ObjectLeafArrayMap.binarySearch(entries.keys, lowKey, 0, currentSize - 1);
		
		Comparable tmp;
		
		if (pos < 0) {
			pos = -(pos + 1);
		} 
		ObjectLeaf currentLeaf = this;
		
		while (currentLeaf != null) {
			while (pos < currentLeaf.entries.currentSize) {
				tmp = currentLeaf.entries.keys[pos];
				if (highKey.compareTo(tmp) < 0)
					return;
				currentLeaf.entries.deleteAtPos(pos);
			}
			currentLeaf = currentLeaf.nextLeaf;
			pos = 0;
		}
	}
	
	public void removeRange2(Comparable lowKey, Comparable highKey) {

		ObjectLeaf currentLeaf = this;
		int pos = 0;
		Comparable tmp;
		boolean continueSearch = true;

		while (currentLeaf != null) {
			if (continueSearch)
				pos = ObjectLeafArrayMap.binarySearch(currentLeaf.entries.keys, lowKey, 0,
						currentLeaf.entries.currentSize - 1);

			if (pos < 0) {
				pos = -(pos + 1);
			} 

			if (pos == currentLeaf.entries.currentSize) {
				currentLeaf = currentLeaf.nextLeaf;
				continue;
			}
			else while (pos < currentLeaf.entries.currentSize) {
				tmp = currentLeaf.entries.keys[pos];
				if (highKey.compareTo(tmp) < 0)
					return;
				currentLeaf.entries.deleteAtPos(pos);
			}
			currentLeaf = currentLeaf.nextLeaf;
			pos = 0;		
			continueSearch = false;
		}
	}

	public void removeRange(Comparable lowKey, Comparable highKey) {
		int currentSize = entries.currentSize;
		if (currentSize == 0)
			return;
		int pos = ObjectLeafArrayMap.binarySearch(entries.keys, lowKey, 0, currentSize - 1);

		Comparable tmp;

		if (pos < 0) {
			pos = -(pos + 1);
		} 

		ObjectLeaf currentLeaf = this;

		//if a value greater than the lowKey was not found , go to the next leaf 
		if(pos == this.entries.currentSize){
			currentLeaf=this.nextLeaf;
		}
		

		//while the consequent leaf contains only values smaller than the lowKey, ignore
		//them and go further
		while(currentLeaf!=null && currentLeaf.entries.currentSize > 0 && 
				lowKey.compareTo(currentLeaf.entries.keys[currentLeaf.entries.currentSize-1]) > 0){
			currentLeaf=currentLeaf.nextLeaf;
		}

		//if we didn't reach the end 
		if(currentLeaf!=null){
			//in the leaf that contains the first value greater than the lowKey 
			//do a binary search to locate the position of that key 
			pos = ObjectLeafArrayMap.binarySearch(currentLeaf.entries.keys, lowKey, 0, currentLeaf.entries.currentSize - 1);
		}
		else{
			//get out of the method;
			return;
		}


		//adapt the pos again
		if (pos < 0) {
			pos = -(pos + 1);
		} 


		while (currentLeaf != null) {
			while (pos < currentLeaf.entries.currentSize) {
				tmp = currentLeaf.entries.keys[pos];
				if (highKey.compareTo(tmp) < 0)
					return;
				currentLeaf.entries.deleteAtPos(pos);
			}
			currentLeaf = currentLeaf.nextLeaf;
			pos = 0;
		}
	}

	public void get(Comparable key, IntPushOperator results) {
		// search in entries
		int continueSearch = entries.get(key, results);
		ObjectLeaf currentLeaf = nextLeaf;
		while (continueSearch != LeafArrayMap.STOP && currentLeaf != null) {
			if (continueSearch == LeafArrayMap.CONTINUE_WITH_BINSEARCH) {
				continueSearch = currentLeaf.entries.get(key, results);
			} else {
				continueSearch = currentLeaf.entries.continueGet(0, key,
						results);
			}
			currentLeaf = currentLeaf.nextLeaf;
		}
		results.thatsallfolks();
	}

	public void toDot(OutputStream dest) {
		StringBuffer sb = new StringBuffer();

		// serialize keys and values
		sb.append("nodeX" + this.hashCode() + " [shape=record,label=\"{{");
		for (int i = 0; i < entries.size(); i++) {
			sb.append("{" + entries.keys[i] + " |");
			sb.append(entries.values[i] + "}");
			if (i != entries.size() - 1) {
				sb.append("|");
			}
		}
		sb.append("}}\"];\n");

		// write keys and values
		try {
			dest.write(sb.toString().getBytes());

			// write next leaf pointer, if any
			if (nextLeaf != null) {
				dest.write(("\"nodeX" + this.hashCode() + "\":"
						+ entries.size() + " -> \"" + "nodeX"
						+ nextLeaf.hashCode() + "\":0;\n").getBytes());
			}

		} catch (IOException e) {
			System.out.println("could not write dotty");
			e.printStackTrace();
		}
	}

	public void queryRange(final Comparable lowKey, final Comparable highKey,
			IntPushOperator results) {
		// start with a query range on this leaf and proceed to next leaf if
		// necessary
		int continueSearch = entries.queryRange(lowKey, highKey, results);
		ObjectLeaf currentLeaf = nextLeaf;
		while (continueSearch != LeafArrayMap.STOP && currentLeaf != null) {
			if (continueSearch == LeafArrayMap.CONTINUE_WITH_SCAN)
				continueSearch = currentLeaf.entries.continueScan(0, highKey,
						results);
			else
				continueSearch = currentLeaf.entries.queryRange(lowKey,
						highKey, results);

			currentLeaf = currentLeaf.nextLeaf;
		}
		results.thatsallfolks();
	}

	public void removeValue(int value) {
		// search for value
		boolean foundValue = false;
		ObjectLeaf currentLeaf = this;

		while (!foundValue && currentLeaf != null) {
			for (int i = 0; i < currentLeaf.entries.size(); i++) {
				if (currentLeaf.entries.values[i] == value) {
					currentLeaf.entries.deleteAtPos(i);
					foundValue = true;
				}
			}
			currentLeaf = currentLeaf.nextLeaf;
		}
	}
	

	public String toString() {
		return "[" + entries.toString() + "]";
	}


	public boolean isEmpty() {
		return entries.size() == 0;
	}

}