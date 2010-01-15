package myDB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metadata.Type;
import metadata.Types;
import myDB.btree.core.btree.BTreeConstants;
import myDB.btree.util.DoubleBTreeMap;
import myDB.btree.util.FloatBTreeMap;
import myDB.btree.util.IntBTreeMap;
import myDB.btree.util.LongBTreeMap;
import myDB.btree.util.ObjectBTreeMap;
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
	
	private static final char EXTENDIBLE_HASH_INDEX = '0';
	
	private static final char B_TREE_INDEX = '1';

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
		
		//Tuan, create index and perform bulk-loading at the same time
		if (namedIndexes.containsKey(indexName))
			throw new IndexAlreadyExistsException();

		Table t; 
		Column c;
		StringBuilder indexDes = new StringBuilder();
		
		try {
			t = storageLayer.getTableByName(tableName);
			c = t.getColumnByName(keyAttributeName);
		}
		catch (NoSuchColumnException nsce) {
			throw new SchemaMismatchException();
		}
		catch (NullPointerException npe) {
			throw new SchemaMismatchException();
		}			
		catch (NoSuchTableException nse) {
			throw new SchemaMismatchException();
		}
		
		indexDes.append(indexName).append("-");
		indexDes.append(tableName).append("-");
		indexDes.append(keyAttributeName);
		
		/*if (!supportRangeQueries) {
			indexDes.append("-0");
			namedIndexes.put(indexName, newIndex(indexDes.toString(), t, c, EXTENDIBLE_HASH_INDEX));
			
		}
		else*/ {
			indexDes.append("-1-");
			indexDes.append(BTreeConstants.DEFAULT_K).append("-");
			indexDes.append(BTreeConstants.DEFAULT_K_STAR);
			namedIndexes.put(indexName, newIndex(indexDes.toString(), t, c, B_TREE_INDEX, supportRangeQueries));
		}
		
		List<String> indexNames = colIndexes.get(c);
		if (indexNames == null) {
			indexNames = new ArrayList<String>();
			colIndexes.put(c, indexNames);
		}
		indexNames.add(indexName);
		
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
		StringBuilder des = new StringBuilder();

		String[] indexes = (String[])namedIndexes.keySet().toArray(new String[0]);

		int i = 0, n = indexes.length;
		for (; i < n; i++) 
			des.append(namedIndexes.get(indexes[i]).describeIndex()).append("$");

		return des.toString();
	}

	/**
	 * Format of the index description:
	 * [indexName]-[tableName]-[attribute]-[identifier]-[other info]
	 */
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
		
		/** Tentative -$BEGIN */
		try {
			Index index = namedIndexes.get(indexName);
			if (!index.supportRangeQueries())
				throw new RangeQueryNotSupportedException();

			TreeIndex tIndex = (TreeIndex)index;
			tIndex.delete(startSearchKey, endSearchKey);
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

	/**
	 * Need to evaluate the performance between two alternatives:
	 * 1 - Serialize a single string (save more I/O time but worse computing)
	 * 2 - Serialize a complex object (don't need to parse string, but I/O is worse)
	 */
	@Override
	public void rebuildAllIndexes() throws IOException {
		FileInputStream fis = new FileInputStream(MyPersistentExtent.INDEXES_METADATA_FILE);
		int i = 0;
		int length = fis.available();
		final byte[] bytes = new byte[length];
		
		fis.read(bytes);
		fis.close();
		
		String str = new String(bytes);
		String[] des = str.split("$");		
		length = des.length;
		
		int k, a, y;
		char indexType;
		String tmp, indexName;
		Table t;
		Column c;
		Index index;
		List<String> list;
				
		try {
			for (i = 0; i < length; i++) {
				tmp = des[i];
				k = tmp.indexOf('-');
				a = tmp.indexOf('-', k);
				y = tmp.indexOf('-', a);
				indexName = tmp.substring(0, k);
				
				
				t = storageLayer.getTableByName(tmp.substring(k + 1, a));
				c = t.getColumnByName(tmp.substring(a + 1, y));
		
				indexType = tmp.charAt(y + 1);
		
				index = newIndex(tmp, t, c, indexType, true);
			
				namedIndexes.put(indexName, index);
				
				if (colIndexes.containsKey(c)) {
					list = colIndexes.get(c);
					list.add(indexName);
				}
				else {
					list = new ArrayList<String>();
					list.add(indexName);
					colIndexes.put(c, list);
				}
			}
		}
		catch (NoSuchTableException nste) {
			throw new IOException();
		}
		catch (NoSuchColumnException nsce) {
			throw new IOException();
		}
		catch (IllegalArgumentException iae) {
			throw new IOException();
		} catch (SchemaMismatchException e) {
			throw new IOException();
		}
	}

	@Override
	public void storeIndexInformation() throws IOException {
		String str = describeAllIndexes();
		byte[] bytes = str.getBytes();
		FileOutputStream fos = new FileOutputStream(MyPersistentExtent.INDEXES_METADATA_FILE);
		fos.write(bytes);
		fos.close();
}

	/**
	 *  '0' - MyExtHashIndex
	 *  '1' - MyCSBTreeIndex
	 * @throws SchemaMismatchException 
	 *  
	 */
	private Index newIndex(String des, Table iTable, Column iCol, int indexType, boolean isRange) throws SchemaMismatchException {

		switch (indexType) {

		case '0': return new MyExtHashIndex(des, iTable, iCol);
		
		case '1': 
			Type type = iCol.getColumnType();
			int pos1 = des.indexOf("-1");
			int pos2 = des.indexOf("-", pos1 + 3);
			
			int k = Integer.parseInt(des.substring(pos1 + 3, pos2));
			
			int k_star = Integer.parseInt(des.substring(pos2 + 1));
			if (type == Types.getDoubleType())
				return new DoubleBTreeMap(des, iTable, iCol, isRange, k, k_star);
			if (type == Types.getFloatType())
				return new FloatBTreeMap(des, iTable, iCol, isRange, k, k_star);
			if (type == Types.getLongType())
				return new LongBTreeMap(des, iTable, iCol, isRange, k, k_star);
			if (type == Types.getIntegerType())
				return new IntBTreeMap(des, iTable, iCol, isRange, k, k_star);
			else
				return new ObjectBTreeMap(des, iTable, iCol, isRange, k, k_star);
		
		default: return null;
		}
	}
	
	public Index getIndexByName(String indexName) {
		return namedIndexes.get(indexName);
	}
}
