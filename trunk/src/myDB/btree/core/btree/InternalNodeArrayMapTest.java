package myDB.btree.core.btree;

import java.util.Random;

import junit.framework.TestCase;

public class InternalNodeArrayMapTest extends TestCase {

	public void testNodeArrayMap() {

		final int no = 10;

		InternalNodeArrayMap am = new InternalNodeArrayMap(10);

		Random rand = new Random(33333);
		for (int i = 0; i < no; i++) {
			int key = rand.nextInt(42);
			BTreeNode value = new myDB.btree.core.btree.InternalNode(5, null);
			System.out.println(key + " " + value.hashCode());

			am.put(key, value);
			System.out.println("POST INSERT:\n" + am);
			System.out.println("--------------------------");

			if (am.get(key) != value) {
				fail("could not find inserted mapping");
			}
		}

		rand = new Random(33333);

		for (int i = 0; i < no; i++) {
			int key = rand.nextInt(42);
			BTreeNode value = am.get(key);

			// validate value corresponds to given key (necessary due to
			// interval semantics of internal b-tree nodes)
			boolean found = false;
			for (int j = 0; j < am.currentSize; j++) {
				if (am.keys[j] == key) {
					found = true;
					break;
				}
			}
			
			System.out.println(key + " " + (value == null? null : value.hashCode()));
			am.delete(key);
			System.out.println("POST DELETE:\n" + am);
			System.out.println("--------------------------");

			if (found && am.get(key) == value) {
				fail("removed key but corresponding pointer did not disappear");
			}
		}
	}
	
}
