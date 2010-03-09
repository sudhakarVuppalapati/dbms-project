package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree directory node.
 * 
 * @author jens/marcos
 */

public class IntInternalNode extends InternalNode implements IntBTreeNode {
	
	protected static final IntInternalNode NULL = new IntInternalNode(0, null);


	protected IntInternalNodeArrayMap entries;

//	protected IntInternalNode nextNode;

	public IntInternalNode(int k, IntInternalNodeArrayMap entries) {
		super(k);
		this.entries = entries;
	}

	public IntInternalNode(IntBTreeNode leftChild, int pivot, IntBTreeNode rightChild,
			int k) {
		super(k);
		this.entries = new IntInternalNodeArrayMap(2 * k);

		entries.nodes[0] = leftChild;
		entries.put(pivot, rightChild);
	}

	public IntSplitInfo add(int key, int value, int lowKey, int highKey, LeafCarrier leafCarrier) {
		// get next node to recurse insertion
		int pos = entries.getIntervalPosition(key);
		IntBTreeNode next = entries.nodes[pos];

		// calculate key interval for next node
		int newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
		int newHighKey = pos == entries.size() ? highKey : entries.keys[pos];
		IntSplitInfo splitInfo = next.add(key, value, newLowKey, newHighKey, leafCarrier);

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

	private IntSplitInfo split() {
		// split node into two:
		int midKey = entries.getMidKey();
		IntInternalNode newNode = new IntInternalNode(k, entries.split());

//		// fix sibling pointers
//		newNode.nextNode = nextNode;
//		nextNode = newNode;

		return new IntSplitInfo(this, midKey, newNode);
	}

	public void remove(int key, int value, int lowKey, int highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(key);
		IntBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			int newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
			int newHighKey = pos == entries.size() ? highKey
					: entries.keys[pos];
			next.remove(key, value, newLowKey, newHighKey);

		}
	}
	
	public void removeRange(int lowKey, int highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(lowKey);
		IntBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			next.removeRange(lowKey, highKey);
		}
	}

	@Override
	public String toString() {
		return "[" + entries.toString() + "]";
	}

	public void get(int key, IntPushOperator results) {
		// recurse into tree
		IntBTreeNode next = entries.get(key);
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
				if (entries.nodes[i] instanceof IntInternalNode) {
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

	public void queryRange(int lowKey, int highKey, IntPushOperator results) {
		// look for lowKey and delegate
		IntBTreeNode next = entries.get(lowKey);
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