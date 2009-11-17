package systeminterface;

import java.io.IOException;
import java.util.Map;

import metadata.Type;
import operator.Operator;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database.
 * 
 */
public interface StorageLayer {

	/**
	 * Create a new table
	 * 
	 * @param tableName
	 *            Name of the table to be created
	 * @param schema
	 *            Schema as map column name -> type
	 * @return Table instance
	 * @throws TableAlreadyExistsException
	 *             Table with same name already in DB
	 */
	public Table createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException;

	/**
	 * Delete a table
	 * 
	 * @param tableName
	 *            Table to be deleted
	 * @throws NoSuchTableException
	 *             Table with given name does not exist
	 */
	public void deleteTable(String tableName) throws NoSuchTableException;

	/**
	 * @param tableName
	 *            Name of table
	 * @return A reference to the table
	 * @throws NoSuchTableException
	 *             Table with given name does not exist
	 */
	public Table getTableByName(String tableName) throws NoSuchTableException;

	/**
	 * @return An operator providing references to all tables in the database
	 */
	public Operator<Table> getTables();

	/**
	 * 
	 * Load tables stored on disk
	 * 
	 * @return Operator of tables
	 * @throws IOException
	 *             IO problem
	 * 
	 */
	public Operator<Table> loadTablesFromExtentIntoMainMemory()
			throws IOException;

	/**
	 * 
	 * Persist tables
	 * 
	 * @param table
	 *            Tables to write
	 * @throws IOException
	 *             IO exception
	 * 
	 */
	public void writeTablesFromMainMemoryBackToExtent(Operator<Table> table)
			throws IOException;

	/**
	 * @param oldName
	 *            Old table name
	 * @param newName
	 *            New table name
	 * @throws TableAlreadyExistsException
	 *             Table already exists
	 * @throws NoSuchTableException
	 *             No such table
	 */
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException;

}
