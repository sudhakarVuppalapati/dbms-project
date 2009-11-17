package systeminterface;

import metadata.Type;
import exceptions.NoSuchColumnException;

/**
 * Row represents horizontal representation for tuple
 * 
 */
public interface Row {

	/**
	 * getColumnCount() return the number of columns in this row
	 * 
	 * @return Number of columns in the row
	 */
	public int getColumnCount();

	/**
	 * Get column type by column name
	 * 
	 * @param columnName
	 *            Column name
	 * @return Type of column
	 * @throws NoSuchColumnException
	 *             If column does not exist
	 */
	public Type getColumnType(String columnName) throws NoSuchColumnException;

	/**
	 * Get value stored in a the column at current row by column name
	 * 
	 * @param columnName
	 *            Name of column
	 * @return An object containing the value stored in that column (with the
	 *         correct type)
	 * @throws NoSuchColumnException
	 *             If column does not exist
	 */
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException;
}
