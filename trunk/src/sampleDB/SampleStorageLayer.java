package sampleDB;

import java.util.HashMap;

import operator.Operator;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database.
 * 
 * @author myahya
 */
public class SampleStorageLayer implements StorageLayer {

	private HashMap<String, Table> tableMap;

	/**
	 * Storage layer constructor
	 */
	public SampleStorageLayer() {

		this.tableMap = new HashMap<String, Table>();
	}

	@Override
	public Table createTable(String tableName)
			throws TableAlreadyExistsException {
		// TODO Auto-generated method stub
		if (!this.tableMap.containsKey(tableName)) {

			this.tableMap.put(tableName, new SampleTable());
			return this.tableMap.get(tableName);

		} else {
			throw new TableAlreadyExistsException();

		}
	}

	/**
	 * Delete a table
	 * 
	 * @param tableName
	 *            name of the table to be deleted
	 */
	public void deleteTable(String tableName) {

		this.tableMap.remove(tableName);
	}

	@Override
	public void deleteTable(Table table) {
		// TODO Auto-generated method stub

	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {

		if (this.tableMap.containsKey(tableName)) {

			return this.tableMap.get(tableName);

		} else {
			throw new NoSuchTableException();

		}
	}

	@Override
	public Operator<Table> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void loadTablesFromExtentIntoMainMemory() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent() {
		// TODO Auto-generated method stub

	}

}
