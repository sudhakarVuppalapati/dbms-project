package myDB.btree.core.btree;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import myDB.btree.util.IntPushOperator;

import junit.framework.TestCase;

public class BTreeTest extends TestCase {

	/**
	 * Tests all b-tree methods for consistency employing a single thread.
	 */
	public void testBTreeSingleThread() {
		final int k = 5;
		final int k_star = 50;
		final int no = 1 * 1000 * 200;
		final boolean validateIntermediaryStates = false;

		BTree btree = new BTree(k, k_star);

		// iterate a couple of times
		for (int j = 0; j < 2; j++) {

			// insert a sequence of key-value pairs into the tree
			SortedMap<Integer, List<Integer>> keySpace = new TreeMap<Integer, List<Integer>>();
			Random rand = new Random(33333);
			for (int i = 0; i < no; i++) {
				int key = rand.nextInt(no);
				int value = no + key;

				btree.add(key, value, null);

				// place it also in keyspace
				List<Integer> values = keySpace.get(key);
				if (values == null) {
					values = new ArrayList<Integer>();
					keySpace.put(key, values);
				}
				values.add(value);

				// check if values are mapped to key
				btree.get(key, new CheckValuesOperator(values, false));

				// validate tree structure
				if (validateIntermediaryStates)
					try {
						validateTree(btree);
					} catch (Exception e) {
						e.printStackTrace();
						fail("btree structure violated while inserting key " + key);
					}
			}

			System.out.println("========================================");
			System.out.println("=  Generating dotty in /tmp/btree" + (j + 1) + ".1.dot  =");
			try {
				btree.toDot(new BufferedOutputStream(new FileOutputStream("/tmp/btree" + (j + 1) + ".1.dot")));
			} catch (FileNotFoundException e) {
				fail("could not write dotty");
			}

			// validate tree after insertions
			try {
				validateTree(btree);
			} catch (Exception e) {
				e.printStackTrace();
				fail("btree structure violated after insertion of keys");
			}

			System.out.println("========================================");

			btree.printStats();
			System.out.println();

			// generate same sequence again and query
			rand = new Random(33333);
			for (int i = 0; i < no; i++) {
				int key = rand.nextInt(no);

				// check if values are the same
				List<Integer> values = keySpace.get(key);
				btree.get(key, new CheckValuesOperator(values, false));
			}

			// range queries (we use some hard-coded values here)
			checkRange(btree, keySpace, 16, 21);
			checkRange(btree, keySpace, 16, 42);

			// query some randomly generated ranges
			for (int i = 0; i < 100; i++) {
				int lowKeyInRange = rand.nextInt(no);
				int highKeyInRange = rand.nextInt(no);

				if (highKeyInRange < lowKeyInRange) {
					// swap
					int tmp = highKeyInRange;
					highKeyInRange = lowKeyInRange;
					lowKeyInRange = tmp;
				}

				checkRange(btree, keySpace, lowKeyInRange, highKeyInRange);
			}

			// delete keys from the b-tree
			rand = new Random(33333);
			for (int i = 0; i < no; i++) {
				int key = rand.nextInt(no);
				Collection<Integer> mappingsToKey = Collections.<Integer> emptySet();
				
				if (keySpace.containsKey(key)) {

					if (j == 0) {
						// in the first round, perform full key removals
						// check that key is still in b-tree
						List<Integer> values = keySpace.get(key);
						btree.get(key, new CheckValuesOperator(values, false));

						// remove key from b-tree and from keyspace
						keySpace.remove(key);
						btree.remove(key);

						// validate tree structure
						if (validateIntermediaryStates)
							try {
								validateTree(btree);
							} catch (Exception e) {
								e.printStackTrace();
								fail("btree structure violated while removing key " + key);
							}
					} else {
						// in the second round, remove one key-value mapping at
						// a time
						int value = no + key;

						// check that key is still in b-tree
						List<Integer> values = keySpace.get(key);
						btree.get(key, new CheckValuesOperator(values, false));

						// remove key-value mapping from b-tree and from
						// keyspace
						values.remove(values.size() - 1);
						if (values.isEmpty()) {
							keySpace.remove(key);
						} else {
							mappingsToKey = values;
						}
						btree.remove(key, value);

						// validate tree structure
						if (validateIntermediaryStates)
							try {
								validateTree(btree);
							} catch (Exception e) {
								e.printStackTrace();
								fail("btree structure violated while removing key " + key);
							}
					}
				}

				// check mappings to key are consistent
				btree.get(key, new CheckValuesOperator(mappingsToKey, false));
			}

			System.out.println("========================================");
			System.out.println("=  Generating dotty in /tmp/btree" + (j + 1) + ".2.dot  =");
			try {
				btree.toDot(new BufferedOutputStream(new FileOutputStream("/tmp/btree" + (j + 1) + ".2.dot")));
			} catch (FileNotFoundException e) {
				fail("could not write dotty");
			}

			// validate tree after deletions
			try {
				validateTree(btree);
			} catch (Exception e) {
				e.printStackTrace();
				fail("btree structure violated after deletion of keys");
			}

			System.out.println("========================================");

			btree.printStats();
			System.out.println();
		}

	}

	private void checkRange(BTree btree, SortedMap<Integer, List<Integer>> keySpace, int lowKey, int highKey) {
		System.out.print("Values between " + lowKey + " and " + highKey + "--> ");

		Set<Integer> expectedValues = new HashSet<Integer>();
		Map<Integer, List<Integer>> expectedMappings = keySpace.subMap(lowKey, highKey + 1);
		for (List<Integer> values : expectedMappings.values()) {
			expectedValues.addAll(values);
		}
		btree.queryRange(lowKey, highKey, new CheckValuesOperator(expectedValues, false));
		System.out.println();
	}

	/**
	 * Verifies if the value range passed to this operator is the same as the
	 * expected value range.
	 * 
	 * @author marcos
	 * 
	 */
	public static class CheckValuesOperator implements IntPushOperator {

		/** values expected by the operator */
		private Collection<Integer> expectedValues;

		/** values obtained by the operator */
		private Set<Integer> obtainedValues;

		/** indicates if results should be printed */
		private boolean print;

		/**
		 * Instantiates a CheckValuesOperator.
		 * 
		 * @param expectedValues
		 */
		public CheckValuesOperator(Collection<Integer> expectedValues, boolean print) {
			this.expectedValues = expectedValues;
			this.obtainedValues = new HashSet<Integer>();
			this.print = print;
		}

		public void pass(int element) {
			obtainedValues.add(element);
			if (print)
				System.out.print(element + " ");
		}

		public void thatsallfolks() {
			if (!obtainedValues.containsAll(expectedValues)) {
				fail("some values were expected but not received by the push operator");
			}
			if (!expectedValues.containsAll(obtainedValues)) {
				obtainedValues.removeAll(expectedValues);
				System.out.println("got the following unexpected values: " + obtainedValues);
				fail("some values were obtained by the push operator but not expected");
			}
			if (print) {
				System.out.println();
				System.out.println("iteration finished");
			}
		}

	}

	/**
	 * Validates the intervals in the btree and throws an exception in case the
	 * tree is no longer valid.
	 * 
	 * @param btree
	 * @throws Exception
	 */
	private void validateTree(BTree btree) throws Exception {
		// check intervals on internal nodes and get number of leaves
		int leafCount;
		if (btree.root instanceof InternalNode) {
			InternalNode root = (InternalNode) btree.root;
			leafCount = checkSubTreeBetween(root, Integer.MIN_VALUE, Integer.MAX_VALUE, btree.k);
		} else {
			Leaf currentLeaf = (Leaf) btree.root;
			leafCount = 0;
			while (currentLeaf != null) {
				currentLeaf = currentLeaf.nextLeaf;
				leafCount++;
			}
		}

		// validate leaf level
		Leaf currentLeaf = btree.firstLeaf;
		int lastKey = Integer.MIN_VALUE;
		int count = 0;
		while (currentLeaf != null) {
			for (int i = 0; i < currentLeaf.entries.size(); i++) {
				if (currentLeaf.entries.keys[i] < lastKey) {
					throw new Exception("leaf level is not sorted: " + lastKey);
				}
				lastKey = currentLeaf.entries.keys[i];
			}
			currentLeaf = currentLeaf.nextLeaf;
			count++;
		}
		if (count < leafCount) {
			throw new Exception("not as many leaves (" + count + ") can be found on the leaf level as expected ("
					+ leafCount + ")");
		}

	}

	private int checkSubTreeBetween(InternalNode node, int minValue, int maxValue, int k) throws Exception {
		int leafCount = 0;
		int lowKey = minValue;
		for (int i = 0; i < node.entries.size(); i++) {
			// check key
			if (node.entries.keys[i] < minValue || node.entries.keys[i] >= maxValue) {
				throw new Exception("key range not valid for node between " + node.entries.keys[0] + " and "
						+ node.entries.keys[node.entries.size() - 1] + " - key: " + node.entries.keys[i]);
			}

			// check subtree
			if (node.entries.nodes[i] instanceof InternalNode) {
				InternalNode next = (InternalNode) node.entries.nodes[i];
				leafCount += checkSubTreeBetween(next, lowKey, node.entries.keys[i], k);
			} else {
				leafCount++;

				// check conditions on leaf interval
				Leaf leaf = (Leaf) node.entries.nodes[i];
				checkLeafBetween(leaf, lowKey, node.entries.keys[i]);
			}

			// update low key
			lowKey = node.entries.keys[i];
		}
		if (node.entries.nodes[node.entries.size()] instanceof InternalNode) {
			InternalNode last = (InternalNode) node.entries.nodes[node.entries.size()];
			leafCount += checkSubTreeBetween(last, lowKey, maxValue, k);
		} else {
			leafCount++;

			// check conditions on leaf interval
			Leaf leaf = (Leaf) node.entries.nodes[node.entries.size()];
			checkLeafBetween(leaf, lowKey, maxValue);
		}
		return leafCount;
	}

	private void checkLeafBetween(Leaf leaf, int lowKey, int highKey) throws Exception {
		Leaf current = leaf;
		while (current != null) {
			// check leaf
			for (int i = 0; i < current.entries.size(); i++) {
				if (current.entries.keys[i] < lowKey || current.entries.keys[i] >= highKey) {
					throw new Exception("key range not valid for leaf [" + current.entries.keys[0] + ", "
							+ current.entries.keys[current.entries.size() - 1] + "] - violating key: "
							+ current.entries.keys[i]);
				}
			}

			if (current.nextLeaf != null && current.nextLeaf.entries.keys[0] < highKey) {
				// leaf is an overflow leaf, so also check it
				current = current.nextLeaf;
			} else {

				current = null;
			}
		}
	}
}
