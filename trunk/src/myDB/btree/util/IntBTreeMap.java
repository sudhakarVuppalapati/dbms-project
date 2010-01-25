package myDB.btree.util;

import myDB.btree.core.btree.BTreeConstants;
import myDB.btree.core.btree.IntBTree;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;


public class IntBTreeMap extends BTreeMap {
		
	private IntBTree btree;

	public IntBTreeMap(String indexDes, Table tableObj, Column colObj, boolean isRange, int k, int k_star) 
	throws SchemaMismatchException {
		super(indexDes, tableObj, colObj, isRange);
		btree = new IntBTree(k, k_star);
	
		//bulk-loading
		int[] colVals;
		try {
			colVals = (int[])colObj.getDataArrayAsObject();
		}
		catch (ClassCastException cce) {
			throw new SchemaMismatchException();
		}
		
		int tmp;
		int n = colObj.getRowCount();
		for (int i = 0; i < n; i++) {
			tmp = colVals[i];
			
			if (tmp == Integer.MAX_VALUE || tmp == Integer.MIN_VALUE) 
				continue;
			
			btree.add(tmp, i);
		}
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		int lowKey, highKey;
		try {
			lowKey = ((Integer)startingKey).intValue();
			highKey = ((Integer)endingKey).intValue();
			if (lowKey > highKey)
				throw new InvalidRangeException();
			else btree.remove(lowKey, highKey, BTreeConstants.ALL_MAPPINGS);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.remove(((Integer)key).intValue(), rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		try {
			btree.remove(((Integer)key).intValue());
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.add(((Integer)key).intValue(), rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}		
	}

	@Override
	public Operator<Row> pointQuery(Object searchKey)
			throws InvalidKeyException {
		MyIntPushOperator results = new MyIntPushOperator();
		try {
			btree.get(((Integer)searchKey).intValue(), results);
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
			btree.get(((Integer)key).intValue(), results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		
		int lowKey, highKey;
		try {
			lowKey = (startingKey != null) ? ((Integer)startingKey).intValue() : Integer.MIN_VALUE;
			highKey = (endingKey != null) ? ((Integer)endingKey).intValue() : Integer.MAX_VALUE;
			if (lowKey > highKey)
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
		int lowKey, highKey;
		try {
			lowKey = (startingKey != null) ? ((Integer)startingKey).intValue() : Integer.MIN_VALUE;
			highKey = (endingKey != null) ? ((Integer)endingKey).intValue() : Integer.MAX_VALUE;	if (lowKey > highKey)
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
			int key = ((Integer)objKey).intValue();
			btree.remove(key, oldRowID);
			btree.add(key, newRowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

}
