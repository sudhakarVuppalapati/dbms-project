package myDB.btree.core.btree;

import java.io.IOException;
import java.io.OutputStream;

import myDB.btree.util.IntPushOperator;

/**
 * Represents a b-tree directory node.
 * 
 * @author jens/marcos
 */

public class ObjectInternalNode extends InternalNode implements ObjectBTreeNode {
	
	protected static final ObjectInternalNode NULL = new ObjectInternalNode(0, null);


	protected ObjectInternalNodeArrayMap entries;

//	protected ObjectInternalNode nextNode;

	public ObjectInternalNode(int k, ObjectInternalNodeArrayMap entries) {
		super(k);
		this.entries = entries;
	}

	public ObjectInternalNode(ObjectBTreeNode leftChild, Comparable pivot, ObjectBTreeNode rightChild,
			int k) {
		super(k);
		this.entries = new ObjectInternalNodeArrayMap(2 * k);

		entries.nodes[0] = leftChild;
		entries.put(pivot, rightChild);
	}

	public ObjectSplitInfo add(Comparable key, int value, Comparable lowKey, Comparable highKey, LeafCarrier leafCarrier) {
		// get next node to recurse insertion
		int pos = entries.getIntervalPosition(key);
		ObjectBTreeNode next = entries.nodes[pos];

		// calculate key interval for next node
		Comparable newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
		Comparable newHighKey = pos == entries.size() ? highKey : entries.keys[pos];
		ObjectSplitInfo splitInfo = next.add(key, value, newLowKey, newHighKey, leafCarrier);

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

	private ObjectSplitInfo split() {
		// split node into two:
		Comparable midKey = entries.getMidKey();
		ObjectInternalNode newNode = new ObjectInternalNode(k, entries.split());

//		// fix sibling pointers
//		newNode.nextNode = nextNode;
//		nextNode = newNode;

		return new ObjectSplitInfo(this, midKey, newNode);
	}

	public void remove(Comparable key, int value, Comparable lowKey, Comparable highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(key);
		ObjectBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			Comparable newLowKey = pos == 0 ? lowKey : entries.keys[pos - 1];
			Comparable newHighKey = pos == entries.size() ? highKey
					: entries.keys[pos];
			next.remove(key, value, newLowKey, newHighKey);
		}
	}
	
	public void removeRange(Comparable lowKey, Comparable highKey) {
		// get next node to recurse deletion
		int pos = entries.getIntervalPosition(lowKey);
		ObjectBTreeNode next = entries.nodes[pos];

		if (next != null) {
			// calculate key interval for next node
			next.removeRange(lowKey, highKey);
		}
	}

	public String toString() {
		return "[" + entries.toString() + "]";
	}

	public void get(Comparable key, IntPushOperator results) {
		// recurse into tree
		ObjectBTreeNode next = entries.get(key);
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
				if (entries.nodes[i] instanceof ObjectInternalNode) {
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

	public void queryRange(Comparable lowKey, Comparable highKey, IntPushOperator results) {
		// look for lowKey and delegate
		ObjectBTreeNode next = entries.get(lowKey);
		next.queryRange(lowKey, highKey, results);
	}

	public boolean isLeaf() {
		return false;
	}

	public boolean isEmpty() {
		return entries.size() == 0 && entries.nodes[0] == null;
	}

}