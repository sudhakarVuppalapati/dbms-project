package systeminterface;

import operator.Operator;
import util.Pair;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;

/**
 * 
 * 
 * @author myahya
 * 
 */
public interface Table {

	/**
	 * Add a new column to a table.<br />
	 * For fixed-size data types
	 * 
	 * @param column
	 * @return columnID unique column identifier in this table instance
	 * @throws ColumnAlreadyExistsException
	 */
	public int addColumn(Column column) throws ColumnAlreadyExistsException;

	/**
	 * inserts a row in this table
	 * 
	 * @param row
	 */
	public void addRow(Row row);

	/**
	 * 
	 * Assign an extent to this table
	 * 
	 * @param extent
	 */
	public void assignExtent(PersistentExtent extent);

	/**
	 * 
	 * delete matching rows
	 * 
	 * @param row
	 */
	public void deleteRow(Row row);

	/**
	 * @param columnID
	 * @throws NoSuchColumnException
	 */
	public void dropColumnByID(int columnID) throws NoSuchColumnException;

	/**
	 * @param columnID
	 * @return a reference to the column
	 * @throws NoSuchColumnException
	 */
	public Column getColumnByID(int columnID) throws NoSuchColumnException;

	/**
	 * @param columnName
	 *            name of column
	 * @return column ID
	 */
	public int getColumnIDByName(String columnName);

	/**
	 * 
	 * @return operator of (columnID, column instance)-pairs
	 */
	public Operator<Pair<Integer, Column>> getColumns();

	/**
	 * @return name of the column
	 */
	public String getName();

	/**
	 * @return an operator that supplies the rows of the table
	 */
	public Operator<Row> getRows();

	/**
	 * @param name
	 *            name of table
	 */
	public void setName(String name);

	/**
	 * @param oldRow
	 *            old row to replace
	 * @param newRow
	 *            new row
	 */
	public void updateRow(Row oldRow, Row newRow);

}
