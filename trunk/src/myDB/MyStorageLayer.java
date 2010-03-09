package myDB;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metadata.Type;
import operator.Operator;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database. Implement this skeleton!!!
 * 
 * 
 */
public class MyStorageLayer implements StorageLayer {

	private static final String TABLES_FILE = "disk/tables.db";

	private Map<String, Table> tables;

	/**
	 * Constructor,
	 */
	public MyStorageLayer() {
		tables = new HashMap<String, Table>();
	}

	@Override
	public Table createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		if (tables.containsKey(tableName))
			throw new TableAlreadyExistsException();

		Table tab = new MyTable(tableName, schema);

		tables.put(tableName, tab);

		return tab;
	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		Table t = tables.remove(tableName);
		if (t == null) {
			throw new NoSuchTableException();
		}
	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {
		Table t = tables.get(tableName);
		if (t != null)
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

		//Before loading the file, clean the in-memory tables
		tables.clear();
		
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(TABLES_FILE);
		}
		/** File not found, build a new database */
		catch (FileNotFoundException ex) {
			return new MyOperator<Table>();
		}

		try {
			ObjectInputStream ois = new ObjectInputStream(
					new BufferedInputStream(fis));
			TablesObject tableObject = (TablesObject) ois.readObject();
			ois.close();
			List<Table> tableLst = tableObject.buildTables();
			String tblName;
			for (Table table : tableLst) {
				tblName = table.getTableName();
				if (!tables.containsKey(tblName))
					tables.put(table.getTableName(), table);
			}
			return new MyOperator<Table>(tableLst);
		}

		catch (ClassCastException ex) {
			ex.printStackTrace();
			throw new IOException();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException();
		} catch (ColumnAlreadyExistsException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException {

		Table t = tables.remove(oldName);

		if (t == null)
			throw new NoSuchTableException();

		if (tables.containsKey(newName))
			throw new TableAlreadyExistsException();

		((MyTable) t).setTableName(newName);

		tables.put(newName, t);
	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent(
			Operator<? extends Table> table) throws IOException {

		TablesObject tableObj = new TablesObject();
		if (tableObj.initialize(table)) {
			try {
				ObjectOutputStream ous = new ObjectOutputStream(
						new BufferedOutputStream(new FileOutputStream(
								TABLES_FILE, false)));
				ous.writeObject(tableObj);
				ous.close();
			} catch (FileNotFoundException ex) {
				throw new IOException();
			} catch (SecurityException ex) {
				throw new IOException();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException();
			}
		}
	}
}
