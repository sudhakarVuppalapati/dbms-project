package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree directory node.
 * 
 * @author jens/marcos
 */

public class DoubleInternalNode extends InternalNode implements DoubleBTreeNode {
	
	protected static final DoubleInternalNode NULL = new DoubleInternalNode(0, null);


	protected DoubleInternalNodeArrayMap entries;

//	protected DoubleInternalNode nextNode;

	public DoubleInternalNode(int k, DoubleInternalNodeArrayMap entries) {
		super(k);
		this.entries = entries;
	}

	public DoubleInternalNode(DoubleBTreeNode leftChild, double pivot, DoubleBTreeNode rightChild,
			int k) {
		super(k);
		this.entries = new DoubleInternalNodeArrayMap(2 * k);

		entries.nodes[0] = leftChild;
		entries.put(pivot, rightChild);
	}

	public DoubleSplitInfo add(double key, int value, double lowKey, double highKey, LeafCarrier leafCarrier) {
		// get next node to recurse insertion
		int pos = entries.getIntervalPosition(key);
		DoubleBTreeNode next = entries.nodes[pos];

		// calculate key interval for next node
		double newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
		double newHighKey = pos == entries.size() ? highKey : entries.keys[pos];
		DoubleSplitInfo splitInfo = next.add(key, value, newLowKey, newHighKey, leafCarrier);

		// after recursion, check for split coming from lower levels
		if (splitInfo != null) {
			// invariant here is that the left is already being pointed to,
			// so we must insert in this node the new (key,rightNode) pair
			// note that put has replace semantics (this is necessary to deal
			// with multiple mappings for the same key)
			entries.put(splitInfo.pivot, splitInfo.rightNode);
			if (entries.size() >= 2 * k) {
				return split();
			}
		}
		return null;
	}

	private DoubleSplitInfo split() {
		// split node into two:
		double midKey = entries.getMidKey();
		DoubleInternalNode newNode = new DoubleInternalNode(k, entries.split());

//		// fix sibling pointers
//		newNode.nextNode = nextNode;
//		nextNode = newNode;

		return new DoubleSplitInfo(this, midKey, newNode);
	}

	public void remove(double key, int value, double lowKey, double highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(key);
		DoubleBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			double newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
			double newHighKey = pos == entries.size() ? highKey
					: entries.keys[pos];
			next.remove(key, value, newLowKey, newHighKey);

			// TODO free at empty on lower level
			if (false && next.isEmpty()) {
				if (next.isLeaf()) {
					DoubleLeaf leaf = (DoubleLeaf) next;
					// handle free at empty on leaf
					// if possible, find next overflow leaf and point to it
					if (leaf.nextLeaf != null
							&& leaf.nextLeaf.entries.keys[0] < newHighKey) {
						// it is an overflow leaf
						// entries.nodes[pos] = leaf.nextLeaf;
						// TODO we should fix previous leaf - would be easy if
						// leaf level was doubly-linked
						// Leaf previousLeaf = (pos > 0)?
					} else {
						// delete child node entry from this node, with
						// corresponding pivot (if any)
						removeChildAtPos(pos);

						// TODO fix previous leaf to point to next
					}
				} else {
					removeChildAtPos(pos);
				}
			}
		}
	}
	
	/**
	 * Delete child node entry from this node, with corresponding pivot (if any)
	 * 
	 * @param pos
	 */
	private void removeChildAtPos(int pos) {
		if (pos == 0) {
			entries.nodes[pos] = null;
		} else {
			entries.deleteAtPos(pos);
		}
	}

	public String toString() {
		return "[" + entries.toString() + "]";
	}

	public void get(double key, IntPushOperator results) {
		// recurse into tree
		DoubleBTreeNode next = entries.get(key);
		next.get(key, results);
	}

	public void toDot(OutputStream dest) {
		StringBuffer sb = new StringBuffer();

		// serialize keys
		sb.append("nodeX" + this.hashCode() + " [shape=record,label=\"{");
		sb.append("{");
		for (int i = 0; i < entries.size(); i++) {
			sb.append("<" + i + "> |");
			sb.append("<key" + i + ">" + entries.keys[i] + "|");
		}
		sb.append("<" + entries.size() + ">}}\"];\n");

		try {
			// write keys
			dest.write(sb.toString().getBytes());

			// serialize nodes reachable from here (if internal nodes)
			for (int i = 0; i < entries.size() + 1; i++) {
				dest.write(("\"nodeX" + this.hashCode() + "\":" + i + " -> \""
						+ "nodeX" + entries.nodes[i].hashCode() + "\";\n")
						.getBytes());
				if (entries.nodes[i] instanceof DoubleInternalNode) {
					entries.nodes[i].toDot(dest);
				}
			}

//			// write next, if any
//			if (nextNode != null) {
//				dest.write(("\"nodeX" + this.hashCode() + "\":"
//						+ entries.size() + " -> \"" + "nodeX"
//						+ nextNode.hashCode() + "\":0;\n").getBytes());
//			}

		} catch (IOException e) {
			System.out.println("could not write dotty");
			e.printStackTrace();
		}

	}

	public void queryRange(double lowKey, double highKey, IntPushOperator results) {
		// look for lowKey and delegate
		DoubleBTreeNode next = entries.get(lowKey);
		next.queryRange(lowKey, highKey, results);
	}

	public boolean isLeaf() {
		return false;
	}

	public boolean isEmpty() {
		return entries.size() == 0 && entries.nodes[0] == null;
	}

}