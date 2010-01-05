package myDB.btree.util;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class JavaTreeMap implements Map {

	private TreeMap<Integer, Integer> treeMap;

	public JavaTreeMap() {
		treeMap = new TreeMap<Integer, Integer>();
	}

	public void delete(int key) {
		treeMap.remove(key);
	}

	public void insert(int key, int value) {
		treeMap.put(key, value);
	}

	public void pointQuery(int key, IntPushOperator results) {
		if (treeMap.containsKey(key)) {
			int value = treeMap.get(key);
			results.pass(value);
		}
	}

	public void rangeQuery(int lowKey, int highKey, IntPushOperator results) {
		SortedMap<Integer, Integer> mapResult = treeMap.subMap(lowKey, highKey + 1);
		Set<java.util.Map.Entry<Integer, Integer>> setView = mapResult.entrySet();
		for (Iterator<java.util.Map.Entry<Integer, Integer>> iterator = setView.iterator(); iterator.hasNext();) {
			results.pass(iterator.next().getValue());
		}
	}

	public long size() {
		return treeMap.size();
	}
}
