/**
 * 
 */
package myDB;

import operator.Operator;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;

/**
 * Java implementation of CSB+ Tree Index, based on samples
 * provided by Prof. Dittrich and the TAs
 * @author attran
 *
 */
public class MyCBSTreeIndex implements TreeIndex {

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

	@Override
	public void delete(Object startingKey, Object endingKey)
			throws InvalidKeyException, InvalidRangeException {
		// TODO Auto-generated method stub
		
	}

}
