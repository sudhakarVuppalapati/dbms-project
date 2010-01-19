package myDB.btree.util;

import myDB.btree.core.btree.FloatBTree;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;


public class FloatBTreeMap extends BTreeMap {
	
	private FloatBTree btree;

	public FloatBTreeMap(String indexDes, Table tableObj, Column colObj, /*boolean isRange,*/ int k, int k_star) 
	throws SchemaMismatchException {
		
		super(indexDes, tableObj, colObj/*, isRange*/);
		btree = new FloatBTree(k, k_star);
		
		//bulk-loading
		float[] colVals;
		try {
			colVals = (float[])colObj.getDataArrayAsObject();
		}
		catch (ClassCastException cce) {
			throw new SchemaMismatchException();
		}
		
		float tmp;
		int n = colObj.getRowCount();
		for (int i = 0; i < n; i++) {
			tmp = colVals[i];
			
			if (tmp == Float.MAX_VALUE || tmp == Float.MIN_VALUE) 
				continue;
			
			btree.add(tmp, i);
		}
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		float lowKey, highKey;
		try {
			lowKey = ((Float)startingKey).floatValue();
			highKey = ((Float)endingKey).floatValue();
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
			btree.remove(((Float)key).floatValue(), rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		try {
			btree.remove(((Float)key).floatValue());
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.add(((Float)key).floatValue(), rowID);
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
			btree.get(((Float)searchKey).floatValue(), results);
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
			btree.get(((Float)key).floatValue(), results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		
		float lowKey, highKey;
		try {
			lowKey =  (startingKey != null) ? ((Float)startingKey).floatValue() : Float.MIN_VALUE;
			highKey = (endingKey != null) ? ((Float)endingKey).floatValue() : Float.MAX_VALUE;
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
		float lowKey, highKey;
		try {
			lowKey = ((Float)startingKey).floatValue();
			highKey = ((Float)endingKey).floatValue();
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
			float key = ((Float)objKey).floatValue();
			btree.remove(key, oldRowID);
			btree.add(key, newRowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

}
