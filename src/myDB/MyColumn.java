/**
 * 
 */
package myDB;

import metadata.Type;
import systeminterface.Column;
import exceptions.NoSuchRowException;

/**
 * @author razvan, attran
 */
public abstract class MyColumn implements Column {

	/** Column name */
	private String name;
	/** Default data capacity */
	protected static final int INIT_CAPACITY = 500;
	/** Load factor when data is full */
	protected static final float FACTOR = 1.5f;

	public MyColumn() {
	}

	public MyColumn(String name) {
		this.name = name;
	}

	@Override
	public String getColumnName() {
		return name;
	}

	@Override
	public void setColumnName(String columnName) {
		name = columnName;
	}

	@Override
	public abstract Type getColumnType();

	@Override
	public abstract Object getElement(int rowID) throws NoSuchRowException;

	@Override
	public abstract int getRowCount();

	@Override
	public abstract Object getDataArrayAsObject();

	/** Extended method */
	// add new data element
	public abstract void add(Object o);

	// mass update
	public abstract void setData(Object data, int curSize);

	// remove data at given rowID position
	public abstract void remove(int rowID);

	// update data at given rowID position
	public abstract void update(int rowID, Object value);

	// remove old data without allocating new column object
	public abstract void eraseOldArray();

	// get undeleted data
	protected abstract Object getActualDataArrayAsObject();
}
