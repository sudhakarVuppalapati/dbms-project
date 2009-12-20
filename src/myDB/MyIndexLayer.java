package myDB;

import operator.Operator;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
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
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dropIndex(String indexName) throws NoSuchIndexException {
		// TODO Auto-generated method stub

	}

	@Override
	public String[] findIndex(String tableName, String keyAttributeName)
			throws SchemaMismatchException, NoSuchTableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertIntoIndex(String indexName, Object key, int rowID)
			throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator<Row> pointQuery(String indexName, Object searchKey)
			throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] pointQueryRowIDs(String indexName, Object searchKey)
			throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<Row> rangeQuery(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void updateIndex(String indexName, Object key, int oldRowID,
			int newRowID) throws NoSuchIndexException, InvalidKeyException {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeIndexInformation() {
		// TODO Auto-generated method stub

	}

}
