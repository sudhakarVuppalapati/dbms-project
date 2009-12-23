package myDB;

import operator.Operator;
import systeminterface.Row;

/**
 * The tree structured direct index
 * @author attran
 *
 */
public interface TreeDirectIndex {
	
	/**
	 * Look up in this index, returning the List of Row objects
	 * with search key matching the supplied one. This method returns
	 * null if not found. Note that to aid the flexibility, we dispatch
	 * the checking of key type to the caller method (usually pointQuery()
	 * in IndexLayer)
	 * @param searchKey
	 * @return Operator of Row types, or null if not found
	 */
	public Operator<Row> pointQuery(Object searchKey);
	/**
	 * Look up in this index the corresponding data entries with 
	 * a search key ranging from startingKey to endingKey. 
	 * Note that to keep it flexible, we have no exception regarding 
	 * the checking of the key type 
	 * @param startingKey
	 * @param endingKey
	 * @return list of rowIDs as Row-type Operator
	 */
	public Operator<Row> rangeQuery(Object startSearchKey, Object endSearchKey);
}
