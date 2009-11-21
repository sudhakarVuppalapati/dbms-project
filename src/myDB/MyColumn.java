/**
 * 
 */
package myDB;

import java.util.ArrayList;
import java.util.List;
import metadata.Type;
import exceptions.NoSuchRowException;
import systeminterface.Column;

/**
 * @author tuanta
 */
public class MyColumn implements Column {

	/* (non-Javadoc)
	 * @see systeminterface.Column#getColumnName()
	 */
	
	/**/
	private String name;
	
	private Type type;
	
	private List data;
	
	@Override
	public String getColumnName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getColumnType()
	 */
	@Override
	public Type getColumnType() {
		return type;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getDataArrayAsObject()
	 */
	@Override
	public Object getDataArrayAsObject() {
		return data.toArray();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getElement(int)
	 */
	@Override
	public Object getElement(int rowID) throws NoSuchRowException {
		return data.get(rowID);
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getRowCount()
	 */
	@Override
	public int getRowCount() {
		return data.size();
	}

	/**
	 * @param name
	 * @param type
	 */
	public MyColumn(String name, Type type) {
		super();
		this.name = name;
		this.type = type;
		this.data = new ArrayList();
	}

	/**
	 * @param name
	 * @param type
	 * @param data
	 */
	public MyColumn(String name, Type type, List data) {
		super();
		this.name = name;
		this.type = type;
		this.data = data;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#setColumnName(java.lang.String)
	 */
	@Override
	public void setColumnName(String columnName) {
		name = columnName;
	}

}
