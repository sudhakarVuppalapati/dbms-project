package myDB.btree.test;

import myDB.btree.core.btree.BTree;

public class ExampleUsageApp {

	/**
	 * @param args ignored.
	 */
	public static void main(String[] args) {

		BTree bTree = new BTree(16, 16);

		// print some info
		bTree.printStats();

		// adding elements
		bTree.add(5, 26);
		bTree.add(912, 42);

		// print some info (again)
		System.out.println();
		bTree.printStats();

	}

}
