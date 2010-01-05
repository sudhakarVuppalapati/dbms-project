package myDB.btree.util;

/**
 * A map that does not do anything.
 * 
 * @author jens
 */
public class DummyMap implements Map {

	public void delete(int key) {
	}

	public void insert(int key, int value) {
	}

	public void pointQuery(int key, IntPushOperator results) {
	}

	public void rangeQuery(int lowKey, int highKey, IntPushOperator results) {
	}

	public long size() {
		return -1;
	}
}
