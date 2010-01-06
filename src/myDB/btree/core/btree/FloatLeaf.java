package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree leaf node.
 * 
 * @author jens/marcos
 */
public class FloatLeaf extends Leaf implements FloatBTreeNode {
	//my addition
	//int type;	
	protected FloatLeafArrayMap entries;

	protected int k_star;

	protected FloatLeaf nextLeaf;

	public FloatLeaf(int k_star, FloatLeafArrayMap entries) {
		this.k_star = k_star;
		this.entries = entries;
		this.nextLeaf = null;
	}

	public FloatLeaf(int k_star) {
		this(k_star, new FloatLeafArrayMap(2 * k_star));
	}

	public FloatSplitInfo add(float key, int value, float lowKey, float highKey,
			LeafCarrier leafCarrier) {
		// search for insertion point: last allowed node of a scan for that key
		// a node is allowed as far as its first key is smaller than the highKey
		// this keeps the strict tree invariant that a leaf pointed to by
		// a given node only contains keys in the interval [lowKey, highKey).

		// HACK: even if a node is empty, we know that its array stil contains
		// the key values it used to have when it was full. That allows us to
		// obtain the key range for a next leaf even if its entries are
		// completely deleted. This is necessary because we know that nodes
		// never get merged.
		boolean continueSearch = entries.tryAdd(key, value);
		FloatLeaf currentLeaf = this;
		while (continueSearch && currentLeaf.nextLeaf != null
				&& currentLeaf.nextLeaf.entries.keys[0] < highKey) {
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
	private FloatSplitInfo split() {
		// split leaf into two new nodes:
		FloatLeaf newLeaf = new FloatLeaf(k_star, entries.split());

		// make sure we point to this new sibling
		newLeaf.nextLeaf = nextLeaf;
		nextLeaf = newLeaf;

		// pivot is first entry in newLeaf keys
		if (newLeaf.entries.keys[0] == entries.keys[entries.size() - 1]) {
			// do not propagate split: pivot does not differentiate leaves
			// this means that the new leaf is an overflow leaf and has no
			// parent node
			return null;
		} else {
			// propagate split: pivot differentiates the two leaves, so the new
			// leaf does not have to be an overflow leaf
			return new FloatSplitInfo(this, newLeaf.entries.keys[0], newLeaf);
		}
	}

	public void remove(float key, int value, float lowKey, float highKey) {
		// continue search on the leaf level until all entries with the given
		// key are guaranteed to be removed
		boolean continueSearch = entries.remove(key, value);
		FloatLeaf currentLeaf = this;

		// HACK: even if a node is empty, we know that its array still contains
		// the key values it used to have when it was full. That allows us to
		// obtain the key range for a next leaf even if its entries are
		// completely deleted. This is necessary because we know that nodes
		// never get merged. (same as in insertion)
		// we use this information below to test if the nextLeaf is still in the
		// allowed range for this deletion
		while (continueSearch && currentLeaf.nextLeaf != null
				&& currentLeaf.nextLeaf.entries.keys[0] < highKey) {
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


	public void get(float key, IntPushOperator results) {
		// search in entries
		int continueSearch = entries.get(key, results);
		FloatLeaf currentLeaf = nextLeaf;
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

	public void queryRange(final float lowKey, final float highKey,
			IntPushOperator results) {
		// start with a query range on this leaf and proceed to next leaf if
		// necessary
		int continueSearch = entries.queryRange(lowKey, highKey, results);
		FloatLeaf currentLeaf = nextLeaf;
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
		FloatLeaf currentLeaf = this;

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