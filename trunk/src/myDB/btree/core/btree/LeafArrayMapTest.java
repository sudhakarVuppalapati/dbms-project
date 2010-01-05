package myDB.btree.core.btree;

import java.util.Random;

import myDB.btree.util.IntPushOperator;

import junit.framework.TestCase;

public class LeafArrayMapTest extends TestCase {

	public static class CheckIntPushOperator implements IntPushOperator {

		private int key;
		private int assertCount;
		private int count;
		
		public CheckIntPushOperator(int key, int assertCount) {
			this.key = key;
			this.assertCount = assertCount;
			this.count = 0;
		}
		
		public void pass(int element) {
			if ((element - 100) != key) {
				fail("could not find correct mappings for key " + key);
			}
			count++;
		}

		public void thatsallfolks() {
			// if needed, assert count
			if (assertCount > 0 && count != assertCount) {
				fail("number of expected values " + assertCount + " did not match actual " + count);
			}
		}
		
	}
	
	public void testLeafArrayMap() {
		final int no = 10;

		LeafArrayMap am = new LeafArrayMap(10);

		// insert some keys
		Random rand = new Random(33333);
		for (int i = 0; i < no; i++) {
			int key = rand.nextInt(42);
			int value = 100 + key;
			System.out.println(key + " " + value);
			am.tryAdd(key, value);
			System.out.println("POST INSERT:\n" + am);
			System.out.println("--------------------------");

			am.get(key, new CheckIntPushOperator(key, -1));
		}

		// delete keys
		rand = new Random(33333);
		for (int i = 0; i < no; i++) {
			int key = rand.nextInt(42);
			int value = 100 + key;
			System.out.println(key + " " + value);
			am.remove(key, value);
			System.out.println("POST DELETE:\n" + am);
			System.out.println("--------------------------");

			am.get(key, new CheckIntPushOperator(key, 0));
		}

	}

}
