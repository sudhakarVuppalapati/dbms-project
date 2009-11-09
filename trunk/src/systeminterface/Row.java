package systeminterface;

import metadata.Type;

/**
 * Row represents horizontal representation for tuple (similar to datablock
 * interface)
 * 
 * @author joerg.schad@gmail.com ISG Universität des Saarlandes
 * @version 0.1
 */
public interface Row {

	/**
	 * getColumnCount() return the number of columns in this row
	 * 
	 * @return number of columns in the row
	 */
	public abstract int getColumnCount();

	/**
	 * getColumnType(int column) used to retrieve the type of a specific column
	 * 
	 * @param columnID
	 *            number of column to be queried
	 * @return type of specified column
	 */
	public abstract Type getColumnType(int columnID);

	/**
	 * getRow(int column) used to retrieve a specific column
	 * 
	 * @param columnID
	 *            number of column to be returned
	 * @return Object array of column tuples
	 * @throws Exception
	 */
	public abstract Object getColumnValue(int columnID) throws Exception;
}
