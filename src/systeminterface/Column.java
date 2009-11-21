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
	 * object. This method MUST return arrays of primitive Java types for the
	 * following data types (see metadata/Types):
	 * 
	 * DOUBLE: return a double[] array <br />
	 * FLOAT: return a float[] array <br />
	 * INTEGER: return an int[] array <br />
	 * LONG: return a long[] array <br />
	 * 
	 * All other types (CHAR, DATE and VARCHAR) return arrays of objects of the
	 * corresponding Java type.
	 * 
	 * For arrays of primitive types, the MIN_VALUE of the corresponding type is
	 * will be designated to correspond to null. As an example, for a null in a
	 * primitive array corresponding to DOUBLE (double []), you would return the
	 * value java.lang.Double.MIN_VALUE. All other methods that return Objects
	 * (such as Column.getElement or Row.getColumnValue still have to return
	 * Java nulls)
	 * 
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
