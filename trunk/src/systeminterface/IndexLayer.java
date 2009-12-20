package systeminterface;

import java.io.IOException;

import operator.Operator;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;

/**
 * The Index interface represents the indexing layer of the database, it sits on
 * top of the storage layer to allow for more efficient access to it.
 * myDB/MyIndexLayer contains a skeleton of a class implementing this interface.
 * You should implement that class and should not modify its constructor. <br />
 * Note that you do not have to persist the contents of the index but only the
 * list of indexes: whenever the database is restarted, you should rebuild all
 * indexes (this is done for simplicity, a real system would persist the
 * indexes). <br />
 * All indexes used here will be indirect ones (the payloads are row IDs).
 */
public interface IndexLayer {

	/**
	 * Create a new index on based on one attribute of a table. Create an new
	 * index on a single attribute of a table. The creator can decide whether
	 * this index will support range queries.
	 * 
	 * @param indexName
	 *            A unique name for the index.
	 * @param tableName
	 *            The name of the table for which this index will be created.
	 * @param keyAttributeName
	 *            The name of the attribute that will serve as the key for this
	 *            index.
	 * @param supportRangeQueries
	 *            A flag indicating whether this index should support range
	 *            queries.
	 * @throws IndexAlreadyExistsException
	 *             An index with the supplied name already exists.
	 * @throws SchemaMismatchException
	 *             The supplied table name & key attribute name do not match.
	 * @throws NoSuchTableException
	 *             The supplied table name does not exist in the database.
	 */
	public void createIndex(String indexName, String tableName,
			String keyAttributeName, boolean supportRangeQueries)
			throws IndexAlreadyExistsException, SchemaMismatchException,
			NoSuchTableException;

	/**
	 * Drop an index. The index itself is destroyed and the corresponding entry
	 * removed.
	 * 
	 * @param indexName
	 *            Name of the index to bee deleted (see createIndex).
	 * @throws NoSuchIndexException
	 *             Index with supplied name does not exist.
	 */
	public void dropIndex(String indexName) throws NoSuchIndexException;

	/**
	 * Perform a point query into the table using the index. Use the index to
	 * retrieve all rows from the table matching the supplied search key. The
	 * search key contains attribute values on which to perform the search in
	 * the index.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param searchKey
	 *            Attribute value for which to search
	 * @return An operator containing all matching rows.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied search key is not valid for the index attribute.
	 */
	public Operator<Row> pointQuery(String indexName, Object searchKey)
			throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Use the index to retrieve all rows from the table matching the supplied
	 * range of search keys (both inclusive).
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param startSearchKey
	 *            Attribute value where search starts.
	 * @param endSearchKey
	 *            Attribute value where search ends.
	 * @return An operator containing all matching rows in the order
	 *         corresponding to the index attributes.
	 * @throws NoSuchIndexException
	 *             Supplied index name does not exist.
	 * @throws InvalidRangeException
	 *             Supplied range is invalid.
	 * @throws InvalidKeyException
	 *             Supplied search keys are not valid for the index attribute.
	 * @throws RangeQueryNotSupportedException
	 *             The index does not support range queries (see createIndex).
	 */
	public Operator<Row> rangeQuery(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException;

	/**
	 * Retrieve all row IDs of rows in the indexed table with a search key
	 * matching the supplied one.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param searchKey
	 *            Attribute value for which to search.
	 * @return An array containing the matching row IDs.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied search key is not valid for the index attributes.
	 */
	public int[] pointQueryRowIDs(String indexName, Object searchKey)
			throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Retrieve all row IDs of rows in the indexed table with a search key
	 * matching the supplied range (start and end search keys inclusive).
	 * 
	 * @param indexName
	 *            Name of the index
	 * @param startSearchKey
	 *            Attribute value where search starts.
	 * @param endSearchKey
	 *            Attribute value where search ends.
	 * @return An array containing the matching row IDs.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist
	 * @throws InvalidRangeException
	 *             Supplied range is invalid
	 * @throws InvalidKeyException
	 *             Supplied search keys are not valid for the index attribute.
	 * @throws RangeQueryNotSupportedException
	 *             This index does not support range queries
	 */
	public int[] rangeQueryRowIDs(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException;

	/**
	 * Return the names of all indexes for the supplied table that on the
	 * supplied attribute name.
	 * 
	 * @param tableName
	 *            Name of the table.
	 * @param keyAttributeName
	 *            The name of the attribute on which the index is created.
	 * @return Matching index names, null if no matches found.
	 * @throws SchemaMismatchException
	 *             The supplied attributes do not match the schema of the table.
	 * @throws NoSuchTableException
	 *             Supplied table name not in database.
	 */
	public String[] findIndex(String tableName, String keyAttributeName)
			throws SchemaMismatchException, NoSuchTableException;

	/**
	 * Insert an entry into an index.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param key
	 *            The key to be inserted.
	 * @param rowID
	 *            The corresponding row ID to be inserted
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied key is not valid for the index attribute.
	 */
	public void insertIntoIndex(String indexName, Object key, int rowID)
			throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Delete all entries in the index with a matching key and rowID.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param key
	 *            Key to be deleted.
	 * @param rowID
	 *            Row ID (payload) to be deleted.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied key is not valid for the index attribute.
	 */
	public void deleteFromIndex(String indexName, Object key, int rowID)
			throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Delete all entries in the index with a matching key (all payloads)
	 * 
	 * @param indexName
	 * @param key
	 *            Key to be deleted.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied key is not valid for the index attribute.
	 */
	public void deleteFromIndex(String indexName, Object key)
			throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Delete the entries corresponding to a range of keys.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param startSearchKey
	 *            Key of the first entry in the range of keys to be deleted.
	 * @param endSearchKey
	 *            Key of the last entry in the range of keys to be deleted.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied key is not valid for the index attribute.
	 * @throws InvalidRangeException
	 *             Supplied range is invalid.
	 * @throws RangeQueryNotSupportedException
	 *             This index does not support range queries.
	 * 
	 */
	public void deleteFromIndex(String indexName, Object startSearchKey,
			Object endSearchKey) throws NoSuchIndexException,
			InvalidKeyException, InvalidRangeException,
			RangeQueryNotSupportedException;

	/**
	 * Update an index entry by replacing one rowID with another.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @param key
	 *            Key of entry to be replaced.
	 * @param oldRowID
	 *            Row ID to be removed.
	 * @param newRowID
	 *            New Row ID to be inserted instead.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 * @throws InvalidKeyException
	 *             Supplied key is not valid for the index attribute.
	 */
	public void updateIndex(String indexName, Object key, int oldRowID,
			int newRowID) throws NoSuchIndexException, InvalidKeyException;

	/**
	 * Describe an index. The format of the returned string and the information
	 * it contains is up to you, we will not strictly test it, but it should be
	 * meaningful. It might also make sense to make it easy to parse, in case
	 * you later need this information.
	 * 
	 * @param indexName
	 *            Name of the index.
	 * @return A string with the description of the index.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 */
	public String describeIndex(String indexName) throws NoSuchIndexException;

	/**
	 * Describe all indexes. The format of the returned string and the
	 * information it contains is up to you, we will not strictly test it, but
	 * it should be meaningful. It might also make sense to make it easy to
	 * parse, in case you later need this information.
	 * 
	 * @return A string with the description of all indexes.
	 */
	public String describeAllIndexes();

	/**
	 * Rebuild all indexes. This method is called from Database.startSystem
	 * 
	 * @throws IOException
	 *             IO exception
	 */
	public void rebuildAllIndexes() throws IOException;

	/**
	 * Store index information.
	 * 
	 * This method stores information about your indexes to disk so that you can
	 * later reconstruct the indexes. This method is called from
	 * Database.shutdownSystem Database.startSystem
	 * 
	 * @throws IOException
	 *             IO exception
	 */
	public void storeIndexInformation() throws IOException;

	/**
	 * Does the supplied index support range queries?
	 * 
	 * @param indexName
	 *            Name of an index.
	 * @return A boolean indicating whether or not the supplied index supports
	 *         range queries.
	 * @throws NoSuchIndexException
	 *             Supplied index does not exist.
	 */
	boolean supportsRangeQueries(String indexName) throws NoSuchIndexException;
}
