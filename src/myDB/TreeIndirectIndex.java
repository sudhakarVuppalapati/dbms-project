package myDB;

/**
 * Tree structured index
 * @author attran
 *
 */
public interface TreeIndirectIndex extends IndirectIndex {
	
	/**
	 * Look up in this index the corresponding data entry of the 
	 * given key, return as a primitive array. Note that to keep 
	 * it flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @return list of rowIDs
	 */
	public int[] pointQueryRowIDs(Object key);
	/**
	 * Look up in this index the corresponding data entries with 
	 * a search key ranging from startingKey to endingKey. 
	 * Note that to keep it flexible, we have no exception regarding 
	 * the checking of the key type 
	 * @param startingKey
	 * @param endingKey
	 * @return list of rowIDs as a primitive array
	 */
	public int[] rangeQueryRowIDs(Object startingKey, Object endingKey);
}
