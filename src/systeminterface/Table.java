package systeminterface;

import metadata.Type;
import operator.Operator;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.SchemaMismatchException;

/**
 *
 */
public interface Table {

	/**
	 * Sets the name of specified column
	 * 
	 * @param oldColumnName
	 *            Old name
	 * @param newColumnName
	 *            New name
	 * @throws ColumnAlreadyExistsException
	 *             ColumnAlreadyExistsException
	 * @throws NoSuchColumnException
	 *             NoSuchColumnException
	 */
	public void renameColumn(String oldColumnName, String newColumnName)
			throws ColumnAlreadyExistsException, NoSuchColumnException;

	/**
	 * Add a new column to a table.
	 * 
	 * @param columnName
	 *            Column name
	 * @param columnType
	 *            Column type
	 * @throws ColumnAlreadyExistsException
	 *             ColumnAlreadyExistsException
	 */
	public void addColumn(String columnName, Type columnType)
			throws ColumnAlreadyExistsException;

	/**
	 * inserts a row in this table
	 * 
	 * @param row
	 *            New row
	 * @return int unique tupleID (can be useful for indexing later on)
	 * @throws SchemaMismatchException
	 *             Schema of supplied row does not match that of table
	 */
	public int addRow(Row row) throws SchemaMismatchException;

	/**
	 * Assign an extent to this table
	 * 
	 * @param extent
	 *            An extent assigned to this table
	 */
	public void assignExtent(PersistentExtent extent);

	/**
	 * Delete rows with matching contents
	 * 
	 * @param row
	 *            Row containing contents to be deleted
	 * @throws NoSuchRowException
	 *             NoSuchRowException
	 * @throws SchemaMismatchException
	 *             SchemaMismatchException
	 */
	public void deleteRow(Row row) throws NoSuchRowException,
			SchemaMismatchException;

	/**
	 * Delete matching rows
	 * 
	 * @param tupleID
	 *            Tuple id of row to be deleted
	 * 
	 * @throws NoSuchRowException
	 *             Row does not exist
	 */
	public void deleteRow(int tupleID) throws NoSuchRowException;

	/**
	 * 
	 * Drop a column
	 * 
	 * @param columnName
	 *            Column to be dropped
	 * @throws NoSuchColumnException
	 *             NoSuchColumn
	 */
	public void dropColumnByName(String columnName)
			throws NoSuchColumnException;

	/**
	 * 
	 * Get a reference to a column
	 * 
	 * @param columnName
	 *            Name of column
	 * @return A reference to the column
	 * @throws NoSuchColumnException
	 *             NoSuchColumn
	 */
	public Column getColumnByName(String columnName)
			throws NoSuchColumnException;

	/**
	 * @param columnNames
	 *            Names of columns to get
	 * @return Operator of column instances
	 * @throws NoSuchColumnException
	 *             Column does not exist
	 */
	public Operator<? extends Column> getColumns(String... columnNames)
			throws NoSuchColumnException;

	/**
	 * @return Operator of column instances
	 */
	public Operator<? extends Column> getAllColumns();

	/**
	 * @return Name of the table
	 */
	public String getTableName();

	/**
	 * @return An operator that supplies the rows of the table
	 */
	public Operator<? extends Row> getRows();

	/**
	 * @param predicate
	 *            The root node of a predicate tree
	 * @return An operator that supplies the rows of the table matching the
	 *         predicate
	 * @throws SchemaMismatchException
	 *             Any mismatch between predicate and table schema
	 */
	public Operator<? extends Row> getRows(PredicateTreeNode predicate)
			throws SchemaMismatchException;

	/**
	 * 
	 * Update a row matching with matching tuple ID using supplied values
	 * 
	 * @param tupleID
	 *            ID of tuple to be updated
	 * @param newRow
	 *            New row
	 * @throws SchemaMismatchException
	 *             Schema of supplied rows does not match that of table
	 * @throws NoSuchRowException
	 *             Referenced row does not exist
	 */
	public void updateRow(int tupleID, Row newRow)
			throws SchemaMismatchException, NoSuchRowException;

	/**
	 * 
	 * Update all rows matching with oldRow using supplied values
	 * 
	 * @param oldRow
	 *            Old row
	 * @param newRow
	 *            New row
	 * @throws SchemaMismatchException
	 *             Schema of supplied rows does not match that of table
	 * @throws NoSuchRowException
	 *             Referenced row does not exist
	 */
	public void updateRow(Row oldRow, Row newRow)
			throws SchemaMismatchException, NoSuchRowException;

}
