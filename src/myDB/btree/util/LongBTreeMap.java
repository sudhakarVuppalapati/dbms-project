package myDB.btree.util;

import myDB.btree.core.btree.LongBTree;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;


public class LongBTreeMap extends BTreeMap {
	
	private LongBTree btree;

	public LongBTreeMap(String indexDes, Table tableObj, Column colObj, boolean isRange, int k, int k_star)
	throws SchemaMismatchException {
		super(indexDes, tableObj, colObj, isRange);
		btree = new LongBTree(k, k_star);
		
		long[] colVals;
		try {
			colVals = (long[])colObj.getDataArrayAsObject();
		}
		catch (ClassCastException cce) {
			throw new SchemaMismatchException();
		}
		
		long tmp;
		int n = colObj.getRowCount();
		for (int i = 0; i < n; i++) {
			tmp = colVals[i];
			
			if (tmp == Long.MAX_VALUE || tmp == Long.MIN_VALUE) 
				continue;
			
			btree.add(tmp, i);
		}
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		long lowKey, highKey;
		try {
			lowKey = ((Long)startingKey).longValue();
			highKey = ((Long)endingKey).longValue();
			if (lowKey > highKey)
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
			btree.remove(((Long)key).longValue(), rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		try {
			btree.remove(((Long)key).longValue());
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.add(((Long)key).longValue(), rowID);
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
			btree.get(((Long)searchKey).longValue(), results);
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
			btree.get(((Long)key).longValue(), results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		
		long lowKey, highKey;
		try {
			lowKey = (startingKey != null) ? ((Long)startingKey).longValue() : Long.MIN_VALUE;
			highKey = (endingKey != null) ? ((Long)endingKey).longValue() : Long.MAX_VALUE;
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
		long lowKey, highKey;
		try {
			lowKey = (startingKey != null) ? ((Long)startingKey).longValue() : Long.MIN_VALUE;
			highKey = (endingKey != null) ? ((Long)endingKey).longValue() : Long.MAX_VALUE;
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
			long key = ((Long)objKey).longValue();
			btree.remove(key, oldRowID);
			btree.add(key, newRowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

}
