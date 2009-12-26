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
	 * given key, return as a primitive array. 
	 * @return list of rowIDs
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class 
	 */
	public int[] pointQueryRowIDs(Object key) throws InvalidKeyException;
	

	/**
	 * Look up in this index, returning the List of Row objects
	 * with search key matching the supplied one. This method returns
	 * null if not found. 
	 * @param searchKey
	 * @return Operator of Row types, or null if not found
	 * @throws InvalidKeyException if the key is not the instance of the 
	 * appropriate class
	 */
	public Operator<Row> pointQuery(Object searchKey) throws InvalidKeyException;
}
