package myDB;

import java.util.Map;

import operator.Operator;
import systeminterface.Column;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;

/**
 * Index Layer. Implement this skeleton. You have to use the constructor shown
 * here.
 * 
 */
public class MyIndexLayer implements IndexLayer {

	private final StorageLayer storageLayer;

	/** Tentative -$BEGIN */

	/** Mapping from index names to index */
	private Map<String, Index> namedIndexes;

	/** Mapping from column to index names */
	private Map<Column, Object> colIndexes;

	/** Tentative -$END */

	/**
	 * 
	 * Constructor, can only know about lower layers. Please do not modify this
	 * constructor
	 * 
	 * @param storageLayer
	 *            A reference to the underlying storage layer
	 * 
	 */
	public MyIndexLayer(StorageLayer storageLayer) {

		this.storageLayer = storageLayer;

	}

	@Override
	public void createIndex(String indexName, String tableName,
			String keyAttributeName, boolean supportRangeQueries)
	throws IndexAlreadyExistsException, SchemaMismatchException,
	NoSuchTableException {

		/**
		 * Tuan - Check the functionality of MyExtHashIndex only. Need to be replaced
		 * by a complete piece of code later
		 */
		/** Tentative -$BEGIN */
		if (supportRangeQueries) {
			return;
		}
		else {
			MyExtHashIndex mehi = new MyExtHashIndex(indexName);
		}
		/** Tentative -$END */

	}

	@Override
	public void deleteFromIndex(String indexName, Object key, int rowID)
	throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromIndex(String indexName, Object key)
	throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromIndex(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidKeyException {
		// TODO Auto-generated method stub

	}

	@Override
	public String describeAllIndexes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String describeIndex(String indexName) throws NoSuchIndexException {
		/** Tentative -$BEGIN */
		try {
			return namedIndexes.get(indexName).describeIndex();
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}
		/** Tentative -$END */
		//return null;
	}

	@Override
	public void dropIndex(String indexName) throws NoSuchIndexException {
		/** Tentative -$BEGIN */
		try {
			namedIndexes.remove(indexName);	
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}		
		/** Tentative -$END */
	}

	@Override
	public String[] findIndex(String tableName, String keyAttributeName)
	throws SchemaMismatchException, NoSuchTableException {
		/** Tentative -$BEGIN */
		try {
			Table t = storageLayer.getTableByName(tableName);
			try {
				Column c = t.getColumnByName(keyAttributeName);
				Object obj = colIndexes.get(c);
				return (String[]) obj;
			} 
			catch (NoSuchColumnException e) {
				//make nonsense to throw SchemaMismatchException here
				throw new SchemaMismatchException();
			}
			catch (ClassCastException cce) {
				throw new SchemaMismatchException();
			}
		}
		catch (NullPointerException npe) {
			throw new NoSuchTableException();
		}
		/** Tentative -$END */
		//return null;
	}

	@Override
	public void insertIntoIndex(String indexName, Object key, int rowID)
	throws NoSuchIndexException, InvalidKeyException {

	}

	@Override
	public Operator<Row> pointQuery(String indexName, Object searchKey)
	throws NoSuchIndexException, InvalidKeyException {
		/** Tentative -$BEGIN */
		try {
			HashIndex index = (HashIndex)namedIndexes.get(indexName);
			if (index.isDirect()) {
				return index.pointQuery(searchKey);
			}
			else {
				int[] rowIDs = index.pointQueryRowIDs(searchKey);
				return ...;
			}

		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}
		/** Tentative -$END */
		//return null;
	}

	@Override
	public int[] pointQueryRowIDs(String indexName, Object searchKey)
			throws NoSuchIndexException, InvalidKeyException {
		
		/** Tentative -$BEGIN */
		try {
			HashIndex index = (HashIndex)namedIndexes.get(indexName);

			return index.pointQueryRowIDs(searchKey);


		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}
		/** Tentative -$END */
		//return null;
	}

	@Override
	public Operator<Row> rangeQuery(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException {
		
		/** Tentative -$BEGIN */
		try {
			TreeIndex index = (TreeIndex)namedIndexes.get(indexName);
			if (!index.supportRangeQueries())
				throw new RangeQueryNotSupportedException();
			if (index.isDirect()) {
				return index.rangQuery(startSearchKey, endSearchKey);
			}
			else {
				int[] rowIDs = index.rangeQueryRowIDs(startSearchKey, endSearchKey);
				return ...;
			}

		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}
		catch (ClassCastException cce) {
			throw new RangeQueryNotSupportedException();
		}
		/** Tentative -$END */
		//return null;
	}

	@Override
	public int[] rangeQueryRowIDs(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidRangeException, InvalidKeyException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rebuildAllIndexes() {
		// TODO Auto-generated method stub

	}


	@Override
	public boolean supportsRangeQueries(String indexName)
	throws NoSuchIndexException {
		/** Tentative -$BEGIN */
		try {
			return namedIndexes.get(indexName).supportRangeQueries();	
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}		
		/** Tentative -$END */
		//return false;
	}


	@Override
	public void updateIndex(String indexName, Object key, int oldRowID,
			int newRowID) throws NoSuchIndexException, InvalidKeyException {
		/** Tentative -$BEGIN */
		try {
			namedIndexes.get(indexName).update(key, oldRowID, newRowID);	
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}		
		/** Tentative -$END */
		//return false;
		}

	@Override
	public void storeIndexInformation() {
		// TODO Auto-generated method stub

	}

}
