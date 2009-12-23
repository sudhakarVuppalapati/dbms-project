/**
 * 
 */
package myDB;

/**
 * An interface providing access to direct indexes
 * @author attran
 *
 */
public interface DirectIndex extends Index {
	/**
	 * Add to this index the content of (key, row), where row is a reference
	 * to a Row object with rowID matching the given ID. The index
	 * is unchanged when the values exist. Note that to keep it 
	 * flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @param key value of search key
	 * @param rowID the rowID to be added
	 */
	public void insert(Object key, int rowID);
	
	/**
	 * Delete from this index the Row object with search key and rowID matching
	 * the supplied ones. The index is unchanged if the key or 
	 * (key, rowID) does not exist.
	 * @param key
	 * @param rowID
	 */
	public void delete(Object key, int rowID);
	
	/**
	 * Update in this index the record with rowIDs matching oldRowID 
	 * and search key matching key. After updating the underlying row should
	 * point to a new Row object. The index is unchanged if such
	 * matchings could not be found. 
	 * @param key
	 * @param oldRowID
	 * @param newRowID
	 */
	public void update(Object key, int oldRowID, int newRowID);

}
