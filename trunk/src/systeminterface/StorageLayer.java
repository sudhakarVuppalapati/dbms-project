package systeminterface;

import operator.Operator;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database.
 * 
 * @author myahya
 */
public interface StorageLayer {

	/**
	 * Create a new table
	 * 
	 * @param tableName
	 *            name of the table to be created
	 * @return table instance
	 * @throws TableAlreadyExistsException
	 */
	public Table createTable(String tableName)
			throws TableAlreadyExistsException;

	/**
	 * Delete a table
	 * 
	 * @param table
	 *            table to be deleted
	 */
	public void deleteTable(Table table);

	/**
	 * @param tableName
	 * @return a reference to the table
	 * @throws NoSuchTableException
	 */
	public Table getTableByName(String tableName) throws NoSuchTableException;

	/**
	 * @return an operator providing references to all tables in the database
	 */
	public Operator<Table> getTables();

	/**
	 * 
	 * Load tables stored on disk
	 * 
	 */
	public void loadTablesFromExtentIntoMainMemory();

	/**
	 * 
	 * persist all tables
	 * 
	 */
	public void writeTablesFromMainMemoryBackToExtent();

}
