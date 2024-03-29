package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree directory node.
 * 
 * @author jens/marcos
 */

public class FloatInternalNode extends InternalNode implements FloatBTreeNode {

	protected static final FloatInternalNode NULL = new FloatInternalNode(0,
			null);

	protected FloatInternalNodeArrayMap entries;

	// protected FloatInternalNode nextNode;

	public FloatInternalNode(int k, FloatInternalNodeArrayMap entries) {
		super(k);
		this.entries = entries;
	}

	public FloatInternalNode(FloatBTreeNode leftChild, float pivot,
			FloatBTreeNode rightChild, int k) {
		super(k);
		this.entries = new FloatInternalNodeArrayMap(2 * k);

		entries.nodes[0] = leftChild;
		entries.put(pivot, rightChild);
	}

	public FloatSplitInfo add(float key, int value, float lowKey,
			float highKey, LeafCarrier leafCarrier) {
		// get next node to recurse insertion
		int pos = entries.getIntervalPosition(key);
		FloatBTreeNode next = entries.nodes[pos];

		// calculate key interval for next node
		float newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
		float newHighKey = pos == entries.size() ? highKey : entries.keys[pos];
		FloatSplitInfo splitInfo = next.add(key, value, newLowKey, newHighKey,
				leafCarrier);

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

	private FloatSplitInfo split() {
		// split node into two:
		float midKey = entries.getMidKey();
		FloatInternalNode newNode = new FloatInternalNode(k, entries.split());

		// // fix sibling pointers
		// newNode.nextNode = nextNode;
		// nextNode = newNode;

		return new FloatSplitInfo(this, midKey, newNode);
	}

	public void remove(float key, int value, float lowKey, float highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(key);
		FloatBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			float newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
			float newHighKey = pos == entries.size() ? highKey
					: entries.keys[pos];
			next.remove(key, value, newLowKey, newHighKey);
		}
	}

	public void removeRange(float lowKey, float highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(lowKey);
		FloatBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			next.removeRange(lowKey, highKey);
		}
	}

	@Override
	public String toString() {
		return "[" + entries.toString() + "]";
	}

	public void get(float key, IntPushOperator results) {
		// recurse into tree
		FloatBTreeNode next = entries.get(key);
		next.get(key, results);
	}

	@Override
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
				if (entries.nodes[i] instanceof FloatInternalNode) {
					entries.nodes[i].toDot(dest);
				}
			}

			// // write next, if any
			// if (nextNode != null) {
			// dest.write(("\"nodeX" + this.hashCode() + "\":"
			// + entries.size() + " -> \"" + "nodeX"
			// + nextNode.hashCode() + "\":0;\n").getBytes());
			// }

		} catch (IOException e) {
			System.out.println("could not write dotty");
			e.printStackTrace();
		}

	}

	public void queryRange(float lowKey, float highKey, IntPushOperator results) {
		// look for lowKey and delegate
		FloatBTreeNode next = entries.get(lowKey);
		next.queryRange(lowKey, highKey, results);
	}

	@Override
	public boolean isLeaf() {
		return false;
	}

	@Override
	public boolean isEmpty() {
		return entries.size() == 0 && entries.nodes[0] == null;
	}

}