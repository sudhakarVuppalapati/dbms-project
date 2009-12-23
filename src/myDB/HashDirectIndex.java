package myDB;

import operator.Operator;
import systeminterface.Row;

public interface HashDirectIndex extends DirectIndex {
	
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

}
