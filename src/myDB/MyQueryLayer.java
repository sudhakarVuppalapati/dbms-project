package myDB;

import java.util.Map;

import operator.Operator;

import metadata.Type;
import systeminterface.Column;
import systeminterface.IndexLayer;
import systeminterface.QueryLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * Query Layer. Implement this skeleton. You have to use the constructor shown
 * here.
 */
public class MyQueryLayer implements QueryLayer {

	private final StorageLayer storageLayer;

	private final IndexLayer indexLayer;

	/**
	 * 
	 * Constructor, can only know about lower layers. Please do not modify this
	 * constructor
	 * 
	 * @param storageLayer
	 *            A reference to the underlying storage layer
	 * @param indexLayer
	 *            A reference to the underlying index layer
	 * 
	 */
	public MyQueryLayer(StorageLayer storageLayer, IndexLayer indexLayer) {

		this.storageLayer = storageLayer;
		this.indexLayer = indexLayer;

	}

	@Override
	public void addColumn(String tableName, String columnName, Type columnType)
			throws NoSuchTableException, ColumnAlreadyExistsException {
		
		Table t = storageLayer.getTableByName(tableName);
		t.addColumn(columnName, columnType);
		
		// TODO Some code in query layers are written here
		
	}

	@Override
	public void createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		
		storageLayer.createTable(tableName, schema);
		
		// TODO Some code in query layers are written here
		
	}

	//Pending
	@Override
	public void deleteRow(String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException {
		
		Table t = storageLayer.getTableByName(tableName);
		
		/**
		 * The following code is too costly, think about it.
		 * One possible alternative: Increase coupling between MyQueryLayer and 
		 * MyTable, create methods like deleteIndexesByRow(Row) in MyIndexLayer
		 * 
		 * Other possible solution: Re-design :)
		 * 
		 * The best compromising solution: Postpone the synchronization between
		 * indexes and base tables. Everytime we delete a row, we create a thread 
		 * to look and update the corresponding data entries. In the meanwhile,
		 * when we search for data of a search key, we need to ensure that the
		 * underlying data is consistent.
		 * 
		 * I'm currently using approach 1
		 * @author tuanta
		 */
		
		//Step 1: Looking for a matching row by full scanning and comparing
		Operator<Row> op = (Operator<Row>) ((MyTable)t).getAllRows();
		String[] colNames = row.getColumnNames();
		int colCnt = colNames.length;
		int rowCnt = ((MyTable)t).getAllRowCount(); 
		boolean[] checked = new boolean[rowCnt];
				
		Row r;
		Column c;
		Type type;
		
		int i, j;
		String tmpCol;		
		boolean found = true;
		
		// TODO Some code in query layers are written here

	}

	//Need review of try catch
	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		Table t = storageLayer.getTableByName(tableName);
		
		Operator<Column> cols = (Operator<Column>) t.getAllColumns();
		Column col;
		String[] indexNames;
		int i, n;
		
		try {
			while ((col = cols.next()) != null) {
				indexNames = indexLayer.findIndex(tableName, col.getColumnName());
				n = indexNames.length;
				for (i = 0; i < n; i++)
					indexLayer.dropIndex(indexNames[i]);				
			}
		} catch (SchemaMismatchException e) {
			e.printStackTrace();		
			//make nosense to throw NoSuchTableException here
			throw new NoSuchTableException();
		} catch (NoSuchIndexException e) {
			e.printStackTrace();
			//make nosense to throw NoSuchTableException here
			throw new NoSuchTableException();
		}
		
		// TODO Some code in query layers are written here

	}

	//Need review of try catch
	@Override
	public void dropColumn(String tableName, String columnName)
			throws NoSuchTableException, NoSuchColumnException {
		
		String[] indexNames;
		
		try {
			indexNames = indexLayer.findIndex(tableName, columnName);
		} catch (SchemaMismatchException e1) {			
			e1.printStackTrace();
			throw new NoSuchColumnException();
		}
		int i, n = indexNames.length;
		
		try {
			for (i = 0; i < n; i++)
			indexLayer.dropIndex(indexNames[i]);
		} catch (NoSuchIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new NoSuchColumnException();
		}	
		
		// TODO Some code in query layers are written here
	}

	@Override
	public void insertRow(String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException {
		Table t = storageLayer.getTableByName(tableName);
		t.addRow(row);
		
		// TODO Some code in query layers are written here
	}

	@Override
	public void renameColumn(String tableName, String oldColumnName,
			String newColumnName) throws NoSuchTableException,
			ColumnAlreadyExistsException, NoSuchColumnException {
		
		Table t = storageLayer.getTableByName(tableName);
		t.renameColumn(oldColumnName, newColumnName);
		
		// TODO Some code in query layers are written here

	}

	@Override
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException {
		storageLayer.renameTable(oldName, newName);
		
		// TODO Some code in query layers are written here
	}

	//Need review try catch
	@Override
	public void updateRow(String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, SchemaMismatchException {
		
		Table t;
		try {
			t = storageLayer.getTableByName(tableName);
		} catch (NoSuchTableException e) {			
			e.printStackTrace();
			throw new SchemaMismatchException();
		}
		t.updateRow(oldRow, newRow);
		
		// TODO Some code in query layers are written here
	}
}
