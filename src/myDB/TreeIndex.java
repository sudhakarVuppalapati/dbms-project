package myDB;

import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import operator.Operator;
import systeminterface.Row;

/**
 * Tree structured index
 * @author attran
 *
 */
public interface TreeIndex extends HashIndex {

	/**
	 * Look up in this index the corresponding data entries with 
	 * a search key ranging from startingKey to endingKey. 
	 * Note that to keep it flexible, we have no exception regarding 
	 * the checking of the key type 
	 * @param startingKey
	 * @param endingKey
	 * @return list of rowIDs as a primitive array
	 */
	public int[] rangeQueryRowIDs(Object startingKey, Object endingKey) 
	throws InvalidKeyException, InvalidRangeException;
	

	/**
	 * Look up in this index, returning the List of Row objects
	 * with search key matching the supplied ones. This method returns
	 * null if not found. Note that to aid the flexibility, we dispatch
	 * the checking of key type to the caller method (usually pointQuery()
	 * in IndexLayer)
	 * @param startingKey
	 * @param endingKey
	 * @return Operator of Row types, or null if not found
	 */
	public Operator<Row> rangeQuery(Object startingKey, Object endingKey) 
	throws InvalidKeyException, InvalidRangeException;
}
