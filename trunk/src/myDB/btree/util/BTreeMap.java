package myDB.btree.util;

import operator.Operator;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import myDB.TreeIndex;
import myDB.btree.core.btree.BTree;

public class BTreeMap implements TreeIndex {

	private BTree btree;

	public BTreeMap(int k, int k_star) {
		btree = new BTree(k, k_star);
	}

	public void delete(int key) {
		btree.remove(key);
	}

	public void insert(int key, int value) {
		btree.add(key, value);
	}

	public void pointQuery(int key, IntPushOperator results) {
		btree.get(key, results);
	}

	public void rangeQuery(int lowKey, int highKey, IntPushOperator results) {
		btree.queryRange(lowKey, highKey, results);
	}

	public long size() {
		return btree.size();
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] rangeQueryRowIDs(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<Row> pointQuery(Object searchKey)
			throws InvalidKeyException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] pointQueryRowIDs(Object key) throws InvalidKeyException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(Object key, int rowID) throws InvalidKeyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String describeIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table getBaseTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean supportRangeQueries() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void update(Object key, int oldRowID, int newRowID)
			throws InvalidKeyException {
		// TODO Auto-generated method stub
		
	}

}
