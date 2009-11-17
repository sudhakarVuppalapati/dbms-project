package systeminterface;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * Column
 * 
 */
public interface Column {

	/**
	 * getColumnName used to retrieve the name of the column
	 * 
	 * @return String Name
	 */
	public String getColumnName();

	/**
	 * getColumnType used to retrieve the type of column
	 * 
	 * @return Type of column
	 */
	public Type getColumnType();

	/**
	 * getDataArrayAsObject used to retrieve an array of the entire column as an
	 * object
	 * 
	 * @return Data array of column tuples
	 */
	public Object getDataArrayAsObject();

	/**
	 * Used to retrieve a specific element in the column
	 * 
	 * @param rowID
	 *            Row number of element to return (see Table.addRow)
	 * @return String Name
	 * @throws NoSuchRowException
	 *             The supplied Row ID does not match a row in the table
	 */
	public Object getElement(int rowID) throws NoSuchRowException;

	/**
	 * 
	 * getRowCount() return the number of rows in this column
	 * 
	 * @return Number of rows in column
	 */
	public int getRowCount();

	/**
	 * Sets the name of the column
	 * 
	 * @param columnName
	 *            Name of column
	 * 
	 *            <b>Note: Column itself is not aware of other columns, hence
	 *            this should be called only via table rename </b>
	 */
	public void setColumnName(String columnName);

}
