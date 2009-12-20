package myDB;

import java.util.Map;

import metadata.Type;
import systeminterface.IndexLayer;
import systeminterface.QueryLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
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
		// TODO Auto-generated method stub

	}

	@Override
	public void createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		// TODO Auto-generated method stub
	}

	@Override
	public void deleteRow(String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		// TODO Auto-generated method stub

	}

	@Override
	public void dropColumn(String tableName, String columnName)
			throws NoSuchTableException, NoSuchColumnException {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertRow(String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException {
		// TODO Auto-generated method stub
	}

	@Override
	public void renameColumn(String tableName, String oldColumnName,
			String newColumnName) throws NoSuchTableException,
			ColumnAlreadyExistsException, NoSuchColumnException {
		// TODO Auto-generated method stub

	}

	@Override
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRow(String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, SchemaMismatchException {
		// TODO Auto-generated method stub

	}

}
