package systeminterface;

import java.util.Map;

import metadata.Type;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * 
 * The Query interface represents the query layer of the database, it sits on
 * top of the storage and index layers. myDB/MyQueryLayer contains a skeleton of
 * a class implementing this interface. You should implement that class and
 * should not modify its constructor. <br />
 * 
 */
public interface QueryLayer {

	/*
	 * DML
	 */

	/**
	 * Insert a new Row.
	 * 
	 * @param tableName
	 *            Name of the table where row is inserted.
	 * @param row
	 *            The row to be inserted
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 */
	public void insertRow(String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException;

	/**
	 * Delete a row completely matching the supplied one.
	 * 
	 * @param tableName
	 *            Name of the table where row is inserted.
	 * @param row
	 *            The row to be deleted.
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 * @throws NoSuchRowException
	 *             No row matching the supplied one exists in the table.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 */
	public void deleteRow(String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException;

	/**
	 * Update the contents of a row.
	 * 
	 * @param tableName
	 *            Name of the table where row is updated.
	 * @param oldRow
	 *            The row to be replaced.
	 * @param newRow
	 *            Replacement row.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 * @throws NoSuchRowException
	 *             No row matching the supplied old row exists in the table.
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 */
	public void updateRow(String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, SchemaMismatchException,
			NoSuchTableException;

	/*
	 * DDL
	 */

	/**
	 * Create a new table.
	 * 
	 * @param tableName
	 *            Name of new table.
	 * @param schema
	 *            Schema of the new table. Column name -> column type.
	 * @throws TableAlreadyExistsException
	 *             A Table with the supplied name already exists.
	 */
	public void createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException;

	/**
	 * 
	 * Delete a table.
	 * 
	 * @param tableName
	 *            Name of the table to delete.
	 * @throws NoSuchTableException
	 *             No table with the supplied table exists.
	 * 
	 */
	public void deleteTable(String tableName) throws NoSuchTableException;

	/**
	 * 
	 * Rename a table.
	 * 
	 * @param oldName
	 *            Table's name.
	 * @param newName
	 *            Tables's new name.
	 * @throws TableAlreadyExistsException
	 *             A table with the new name already exists.
	 * @throws NoSuchTableException
	 *             No table with supplied old name exists.
	 */
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException;

	/**
	 * 
	 * Rename a column.
	 * 
	 * @param tableName
	 *            Name of the table.
	 * @param oldColumnName
	 *            Column's name.
	 * @param newColumnName
	 *            Column's new name.
	 * @throws NoSuchTableException
	 *             No table with the supplied name exists.
	 * @throws ColumnAlreadyExistsException
	 *             A column with the new name already exists in the table.
	 * @throws NoSuchColumnException
	 *             No column with the old name exists in the table.
	 */
	public void renameColumn(String tableName, String oldColumnName,
			String newColumnName) throws NoSuchTableException,
			ColumnAlreadyExistsException, NoSuchColumnException;

	/**
	 * 
	 * Add a new column to a table.
	 * 
	 * @param tableName
	 *            Name of the table.
	 * @param columnName
	 *            Name of the new column.
	 * @param columnType
	 *            Type of the new column.
	 * @throws NoSuchTableException
	 *             No table with the supplied name exists.
	 * @throws ColumnAlreadyExistsException
	 *             A column with the same name already exists in the table.
	 */
	public void addColumn(String tableName, String columnName, Type columnType)
			throws NoSuchTableException, ColumnAlreadyExistsException;

	/**
	 * 
	 * Drop a column from a table.
	 * 
	 * @param tableName
	 *            Name of the table.
	 * @param columnName
	 *            Name of the column.
	 * @throws NoSuchTableException
	 *             No table with the supplied name exists.
	 * @throws NoSuchColumnException
	 *             No column with the supplied name exists.
	 */
	public void dropColumn(String tableName, String columnName)
			throws NoSuchTableException, NoSuchColumnException;
}
