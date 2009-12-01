package myDB;

import java.io.IOException;
import java.util.HashMap;
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

	private HashMap<String,Table> tables;
	/**
	 * Constructor,
	 */
	public MyStorageLayer() {
		tables=new HashMap<String,Table>();
	}

	@Override
	public Table createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		if(tables.containsKey(tableName))
			throw new TableAlreadyExistsException();
		
		Table tab=new MyTable(tableName,schema);
		tables.put(tableName,tab);
		
		return tab;
	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		if(tables.containsKey(tableName)){
			tables.remove(tableName);
		}
		throw new NoSuchTableException();
			
	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {
		Table t=tables.get(tableName);
		if(t!=null)
			return t;
		throw new NoSuchTableException();
	}

	@Override
	public Operator<Table> getTables() {
		return new MyOperator<Table>(tables.values());
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
		Table t=tables.remove(oldName);
		if(t==null)
			throw new NoSuchTableException();
		
		if(tables.containsKey(newName))
			throw new TableAlreadyExistsException();
		
		tables.put(newName,t);
	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent(
			Operator<? extends Table> table) throws IOException {
		// TODO Auto-generated method stub

	}

}
