package myDB.btree.util;

import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.SchemaMismatchException;
import myDB.MyTable;
import myDB.TreeIndex;

public abstract class BTreeMap implements TreeIndex {

	protected final MyTable table;
	
	private String des;
		
	private final boolean range;
	
	public BTreeMap(String indexDes, Table tableObj, Column colObj, boolean isRange) 
	throws SchemaMismatchException {
		des = indexDes;
		table = (MyTable)tableObj;
		range = isRange;
	}

	public abstract long size();

	@Override
	public abstract void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException;

	@Override
	public abstract Operator<Row> rangeQuery(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException ;

	@Override
	public abstract int[] rangeQueryRowIDs(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException;

	@Override
	public abstract Operator<Row> pointQuery(Object searchKey)
			throws InvalidKeyException;

	@Override
	public abstract int[] pointQueryRowIDs(Object key) throws InvalidKeyException;

	@Override
	public abstract void delete(Object key, int rowID) throws InvalidKeyException;

	@Override
	public abstract void delete(Object key) throws InvalidKeyException;

	@Override
	public String describeIndex() {
		return des;
	}

	@Override
	public Table getBaseTable() {
		return table;
	}

	@Override
	public abstract void insert(Object key, int rowID) throws InvalidKeyException;

	@Override
	public boolean supportRangeQueries() {
		return range;
	}

	@Override
	public abstract void update(Object key, int oldRowID, int newRowID)
			throws InvalidKeyException;

}
