package myDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import metadata.Type;

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
	private Map<Column, List<String>> colIndexes;

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

		/** TENTATIVE -$BEGIN */
		namedIndexes = new HashMap<String, Index>();
		colIndexes = new HashMap<Column, List<String>>();
		/** TENTATIVE -$END */
	}

	@Override
	public void createIndex(String indexName, String tableName,
			String keyAttributeName, boolean supportRangeQueries)
	throws IndexAlreadyExistsException, SchemaMismatchException,
	NoSuchTableException {

		/** Tentative -$BEGIN */
		/**
		 * Tuan - This code fragment is only to check the functionality of 
		 * MyExtHashIndex. Need to be replaced later
		 */
		
		if (supportRangeQueries) {
			return;
		}
		else {
			try {
				if (namedIndexes.containsKey(indexName))
					throw new IndexAlreadyExistsException();
				
				Table t = storageLayer.getTableByName(tableName);
				Column c = t.getColumnByName(keyAttributeName);
				Type type = c.getColumnType();
				MyExtHashIndex mehi = new MyExtHashIndex(indexName, type, t);
				namedIndexes.put(indexName, mehi);
				
				//Add to column/index-names mapping
				List<String> list = colIndexes.get(c);
				if (list != null) {
					list.add(indexName);
				}
				else {
					list = new ArrayList<String>();
					list.add(indexName);
					colIndexes.put(c, list);
				}			
			}
			catch (NoSuchColumnException nsce) {
				throw new SchemaMismatchException();
			}
			catch (NullPointerException npe) {
				throw new SchemaMismatchException();
			}			
			catch (Exception e) {
				throw new NoSuchTableException();
			}
		}
		/** Tentative -$END */

	}

	@Override
	public void deleteFromIndex(String indexName, Object key, int rowID)
	throws NoSuchIndexException, InvalidKeyException {
		/** Tentative -$BEGIN */
		try {
			namedIndexes.get(indexName).delete(key, rowID);	
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}		
		/** Tentative -$END */
		//return false;

	}

	@Override
	public void deleteFromIndex(String indexName, Object key)
	throws NoSuchIndexException, InvalidKeyException {
		/** Tentative -$BEGIN */
		try {
			namedIndexes.get(indexName).delete(key);	
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}		
		/** Tentative -$END */
		//return false;
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
				List<String> obj = colIndexes.get(c);
				String[] indexes = null;
				if (obj!= null) {
					indexes = obj.toArray(new String[obj.size()]);
				}
				return indexes;
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
		try {
			namedIndexes.get(indexName).insert(key, rowID);
		}
		catch (NullPointerException npe) {
			throw new NoSuchIndexException();
		}
	}

	@Override
	public Operator<Row> pointQuery(String indexName, Object searchKey)
	throws NoSuchIndexException, InvalidKeyException {
		/** Tentative -$BEGIN */
		try {
			return ((HashIndex)namedIndexes.get(indexName)).pointQuery(searchKey);
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
			Index index = namedIndexes.get(indexName);
			if (!index.supportRangeQueries())
				throw new RangeQueryNotSupportedException();
			
			TreeIndex tIndex = (TreeIndex)index;
			return tIndex.rangeQuery(startSearchKey, endSearchKey);
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
			InvalidRangeException, InvalidKeyException, RangeQueryNotSupportedException {
		/** Tentative -$BEGIN */
		try {
			Index index = namedIndexes.get(indexName);
			if (!index.supportRangeQueries())
				throw new RangeQueryNotSupportedException();
			
			TreeIndex tIndex = (TreeIndex)index;
			return tIndex.rangeQueryRowIDs(startSearchKey, endSearchKey);
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
	public void deleteFromIndex(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidKeyException, InvalidRangeException,
			RangeQueryNotSupportedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rebuildAllIndexes() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeIndexInformation() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
