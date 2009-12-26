/**
 * 
 */
package myDB;

import systeminterface.Table;
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
	
	/**
	 * Add to this index the content of (key, rowID). The index
	 * is unchanged when the values exist. 
	 * @param key value of search key
	 * @param rowID the rowID to be added
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class
	 */
	public void insert(Object key, int rowID) throws InvalidKeyException;
	
	/**
	 * Delete from this index the rowID with search key matching
	 * the supplied one. The index is unchanged if the key or 
	 * (key, rowID) does not exist.
	 * @param key
	 * @param rowID
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class
	 */
	public void delete(Object key, int rowID) throws InvalidKeyException;
	
	/**
	 * Delete from this index the data entry corresponding to the given
	 * search key
	 * @param key
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class
	 */
	public void delete(Object key) throws InvalidKeyException;	
	/**
	 * Update in this index the record with rowIDs matching oldRowID 
	 * and search key matching key. The index is unchanged if such
	 * matchings could not be found. 
	 * @param key
	 * @param oldRowID
	 * @param newRowID
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class	 
	 */
	public void update(Object key, int oldRowID, int newRowID) throws InvalidKeyException;
	
	/** TENTATIVE -$BEGIN */
	/**
	 * Return the reference to base table. Might not be implemented in some indexes, so be
	 * cautious to use this method
	 */
	public Table getBaseTable();
	/** TENTATIVE -$END */
}
