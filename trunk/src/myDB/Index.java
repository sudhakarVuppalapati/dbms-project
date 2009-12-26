/**
 * 
 */
package myDB;

import exceptions.InvalidKeyException;

/**
 * A general interface for all types of indexes. Every access to indexes
 * in MyIndexLayer should go through this interface
 * @author attran
 *
 */
public interface Index {
	
	/** Return true if this index support range queries, false otherwise */
	public boolean supportRangeQueries();
	
	/** Describe this index */
	public String describeIndex();

	/** The method checking if this index is direct or not */
	public boolean isDirect();
	
	/**
	 * Add to this index the content of (key, rowID). The index
	 * is unchanged when the values exist. Note that to keep it 
	 * flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @param key value of search key
	 * @param rowID the rowID to be added
	 */
	public void insert(Object key, int rowID) throws InvalidKeyException;
	
	/**
	 * Delete from this index the rowID with search key matching
	 * the supplied one. The index is unchanged if the key or 
	 * (key, rowID) does not exist.
	 * @param key
	 * @param rowID
	 */
	public void delete(Object key, int rowID) throws InvalidKeyException;
	
	/**
	 * Delete from this index the data entry corresponding to the given
	 * search key
	 * @param key
	 * @throws InvalidKeyException
	 */
	public void delete(Object key) throws InvalidKeyException;	
	/**
	 * Update in this index the record with rowIDs matching oldRowID 
	 * and search key matching key. The index is unchanged if such
	 * matchings could not be found. 
	 * @param key
	 * @param oldRowID
	 * @param newRowID
	 */
	public void update(Object key, int oldRowID, int newRowID) throws InvalidKeyException;
}
