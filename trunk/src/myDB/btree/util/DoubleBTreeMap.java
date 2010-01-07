package myDB.btree.util;

import myDB.btree.core.btree.BTreeConstants;
import myDB.btree.core.btree.DoubleBTree;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;


public class DoubleBTreeMap extends BTreeMap {
	
	private DoubleBTree btree;

	public DoubleBTreeMap(String indexDes, Table tableObj, Column colObj, boolean isRange, int k, int k_star)
	throws SchemaMismatchException {
		super(indexDes, tableObj, colObj, isRange);
		btree = new DoubleBTree(k, k_star);
		
		//bulk-loading
		double[] colVals;
		try {
			colVals = (double[])colObj.getDataArrayAsObject();
		}
		catch (ClassCastException cce) {
			throw new SchemaMismatchException();
		}
		
		double tmp;
		int n = colObj.getRowCount();
		for (int i = 0; i < n; i++) {
			tmp = colVals[i];
			
			if (tmp == Double.MAX_VALUE || tmp == Double.MIN_VALUE) 
				continue;
			
			btree.add(tmp, i);
		}
		
	}

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		double lowKey, highKey;
		try {
			lowKey = ((Double)startingKey).doubleValue();
			highKey = ((Double)endingKey).doubleValue();
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
			btree.remove(((Double)key).doubleValue(), rowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

	@Override
	public void delete(Object key) throws InvalidKeyException {
		try {
			btree.remove(((Double)key).doubleValue());
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		
	}

	@Override
	public void insert(Object key, int rowID) throws InvalidKeyException {
		try {
			btree.add(((Double)key).doubleValue(), rowID);
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
			btree.get(((Double)searchKey).doubleValue(), results);
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
			btree.get(((Double)key).doubleValue(), results);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
		return results.getData();
	}

	@Override
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		
		double lowKey, highKey;
		try {
			lowKey = ((Double)startingKey).doubleValue();
			highKey = ((Double)endingKey).doubleValue();
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
		double lowKey, highKey;
		try {
			lowKey = ((Double)startingKey).doubleValue();
			highKey = ((Double)endingKey).doubleValue();
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
			double key = ((Double)objKey).doubleValue();
			btree.remove(key, oldRowID);
			btree.add(key, newRowID);
		}
		catch (ClassCastException cce) {
			throw new InvalidKeyException();
		}
	}

}
