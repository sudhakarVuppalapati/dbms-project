package myDB;

import exceptions.InvalidKeyException;
import operator.Operator;
import systeminterface.Row;

/**
 * Hashing-based indirect index
 * @author attran
 *
 */
public interface HashIndex extends Index {
	/**
	 * Look up in this index the corresponding data entry of the 
	 * given key, return as a primitive array. Note that to keep 
	 * it flexible, we have no exception regarding the checking of 
	 * the key type 
	 * @return list of rowIDs
	 */
	public int[] pointQueryRowIDs(Object key) throws InvalidKeyException;
	

	/**
	 * Look up in this index, returning the List of Row objects
	 * with search key matching the supplied one. This method returns
	 * null if not found. Note that to aid the flexibility, we dispatch
	 * the checking of key type to the caller method (usually pointQuery()
	 * in IndexLayer)
	 * @param searchKey
	 * @return Operator of Row types, or null if not found
	 */
	public Operator<Row> pointQuery(Object searchKey) throws InvalidKeyException;
}
