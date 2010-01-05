package myDB.btree.util;

/**
 * Simple map interface for wrapping existing indexes.
 * 
 * @author jens
 */
public interface Map {
	public void insert(int key, int value);

	public void delete(int key);

	public void pointQuery(int key, IntPushOperator results);

	public void rangeQuery(int lowKey, int highKey, IntPushOperator results);
	
	public long size();
}
