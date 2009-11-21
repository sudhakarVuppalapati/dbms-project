package myDB;

import java.io.IOException;
import java.util.Map;

import metadata.Type;
import operator.Operator;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database. Implement this skeleton!!!
 * 
 * 
 */
public class MyStorageLayer implements StorageLayer {

	/**
	 * Constructor,
	 */
	public MyStorageLayer() {

	}

	@Override
	public Table createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		// TODO Auto-generated method stub

	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<Table> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<Table> loadTablesFromExtentIntoMainMemory()
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent(
			Operator<? extends Table> table) throws IOException {
		// TODO Auto-generated method stub

	}

}
