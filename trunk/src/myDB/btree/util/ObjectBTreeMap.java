package myDB.btree.util;

import myDB.MyNull;
import myDB.btree.core.btree.ObjectBTree;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;


public class ObjectBTreeMap extends BTreeMap {
	
	private ObjectBTree btree;

	public ObjectBTreeMap(String indexDes, Table tableObj, Column colObj, boolean isRange, int k, int k_star) 
	throws SchemaMismatchException {
		super(indexDes, tableObj, colObj, isRange);
		btree = new ObjectBTree(k, k_star);
	
		//bulk-loading
	
		
		Object[] colVals;
		try {
			colVals = (Object[])colObj.getDataArrayAsObject();
		}
		catch (ClassCastException cce) {
			throw new SchemaMismatchException();
		}
		
		int n = colObj.getRowCount();
		for (int i = 0; i < n; i++) {
			Comparable o = (Comparable)colVals[i];
			
			//Probably we need to implements 
			if (o == null || o == MyNull.NULLOBJ) 
				continue;
			
			btree.add(o, i);
		}
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		Comparable lowKey, highKey;
		try {
			lowKey = (Comparable)startingKey;
			highKey = (Comparable)endingKey;
			if (lowKey.compareTo(highKey) > 0)
				throw new InvalidRangeException();
			else btree.remove(lowKey, highKey);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.remove((Comparable)key, rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		try {
			btree.remove((Comparable)key);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.add((Comparable)key, rowID);
		}
		catch (ClassCastException cce) {
			cce.printStackTrace();
			throw new InvalidKeyException();
		}		
	}

	@Override
	public Operator<Row> pointQuery(Object searchKey)
			throws InvalidKeyException {
		MyIntPushOperator results = new MyIntPushOperator();
		try {
			btree.get((Comparable)searchKey, results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return table.getRows(results.getData());
	}

	@Override
	public int[] pointQueryRowIDs(Object key) throws InvalidKeyException {
		MyIntPushOperator results = new MyIntPushOperator();
		try {
			btree.get((Comparable)key, results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		
		Comparable lowKey, highKey;
		try {
			lowKey = (startingKey != null) ? (Comparable)startingKey : Infinity.MIN_VALUE;
			highKey = (endingKey != null) ? (Comparable)endingKey : Infinity.MIN_VALUE;
			if (lowKey.compareTo(highKey) > 0)
				throw new InvalidRangeException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
				
		MyIntPushOperator results = new MyIntPushOperator();
		try {
			btree.queryRange(lowKey, highKey, results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return table.getRows(results.getData());
	}

	@Override
	public int[] rangeQueryRowIDs(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		Comparable lowKey, highKey;
		try {
			lowKey = (Comparable)startingKey;
			highKey = (Comparable)endingKey;
			if (lowKey.compareTo(highKey) > 0)
				throw new InvalidRangeException();
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
				
		MyIntPushOperator results = new MyIntPushOperator();
		try {
			btree.queryRange(lowKey, highKey, results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public long size() {
		return btree.size();
	}

	@Override
	public void update(Object objKey, int oldRowID, int newRowID)
			throws InvalidKeyException {
		try {
			Comparable key = (Comparable)objKey;
			btree.remove(key, oldRowID);
			btree.add(key, newRowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

}
