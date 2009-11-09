package systeminterface;

import metadata.Type;

/**
 * Column 
 * 
 * @author joerg.schad@gmail.com ISG Universität des Saarlandes
 * @version 0.1
 */
public interface Column {

	/**
	 * getColumnType used to retrieve the type of column
	 * 
	 * @return type of column
	 */
	public abstract Type getColumnType();

	/**
	 * getDataArrayAsObject used to retrieve an array of the entire column as an
	 * object
	 * 
	 * @return data array of column tuples
	 * @throws Exception
	 */
	public abstract Object getDataArrayAsObject() throws Exception;

	/**
	 * used to retrieve a specific element in the column
	 * 
	 * @param rowNumber
	 *            row number of element to return
	 * @return String Name
	 */
	public abstract Object getElement(int rowNumber);

	/**
	 * getName used to retrieve the name of the column used to retrieve a
	 * specific column Name
	 * 
	 * @return String Name
	 */
	public abstract String getName();

	/**
	 * 
	 * getRowCount() return the number of rows in this column returns the number
	 * of rows in this column
	 * 
	 * @return number of rows in column
	 */
	public abstract int getRowCount();

	/**
	 * sets the name
	 * 
	 * @param columnName
	 *            name of column
	 */
	public abstract void setColumnName(String columnName);

	/**
	 * setColumnType(Type columnType) set the type set the type of this column
	 * 
	 * @param columnType
	 *            type of column
	 */
	public abstract void setColumnType(Type columnType);

}
