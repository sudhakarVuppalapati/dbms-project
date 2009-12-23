package myDB;

/**
 * Interface to indirect index, or indexes of which data entries 
 * contain row IDs only
 * @author attran
 *
 */
public interface IndirectIndex extends Index {
	
	/**
	 * Add to this index the content of (key, rowID). The index
	 * is unchanged when the values exist. Note that to keep it 
	 * flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @param key value of search key
	 * @param rowID the rowID to be added
	 */
	public void insert(Object key, int rowID);
	
	/**
	 * Delete from this index the rowID with search key matching
	 * the supplied one. The index is unchanged if the key or 
	 * (key, rowID) does not exist.
	 * @param key
	 * @param rowID
	 */
	public void delete(Object key, int rowID);
	
	/**
	 * Update in this index the record with rowIDs matching oldRowID 
	 * and search key matching key. The index is unchanged if such
	 * matchings could not be found. 
	 * @param key
	 * @param oldRowID
	 * @param newRowID
	 */
	public void update(Object key, int oldRowID, int newRowID);
}
