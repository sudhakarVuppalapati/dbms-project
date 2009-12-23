package myDB;

/**
 * Hashing-based indirect index
 * @author attran
 *
 */
public interface HashIndirectIndex extends IndirectIndex {
	/**
	 * Look up in this index the corresponding data entry of the 
	 * given key, return as a primitive array. Note that to keep 
	 * it flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @return list of rowIDs
	 */
	public int[] pointQueryRowIDs(Object key);
}
