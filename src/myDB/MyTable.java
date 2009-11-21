/**
 * 
 */
package myDB;

import java.util.Collection;
import java.util.Map;

import metadata.Type;
import operator.Operator;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.SchemaMismatchException;
import systeminterface.Column;
import systeminterface.PersistentExtent;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.Table;

/**
 * @author tuanta
 *
 */
public class MyTable implements Table {

	/* (non-Javadoc)
	 * @see systeminterface.Table#addColumn(java.lang.String, metadata.Type)
	 */
	private String name;
	private Collection<Row> rows;
	private Map<String, Column> cols;
	@Override
	public void addColumn(String columnName, Type columnType)
			throws ColumnAlreadyExistsException {

		if (!cols.containsKey(columnName)) {
			cols.put(columnName, new MyColumn(columnName, columnType));
		}
		else 
			throw new ColumnAlreadyExistsException();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#addRow(systeminterface.Row)
	 */
	@Override
	public int addRow(Row row) throws SchemaMismatchException {
		return 0;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#assignExtent(systeminterface.PersistentExtent)
	 */
	@Override
	public void assignExtent(PersistentExtent extent) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#deleteRow(systeminterface.Row)
	 */
	@Override
	public void deleteRow(Row row) throws NoSuchRowException,
			SchemaMismatchException {
		
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#deleteRow(int)
	 */
	@Override
	public void deleteRow(int tupleID) throws NoSuchRowException {
		
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#dropColumnByName(java.lang.String)
	 */
	@Override
	public void dropColumnByName(String columnName)
			throws NoSuchColumnException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getAllColumns()
	 */
	@Override
	public Operator<Column> getAllColumns() {
		Operator<Column> opCol = new MyOperator<Column>(cols.values());
		return opCol;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getColumnByName(java.lang.String)
	 */
	@Override
	public Column getColumnByName(String columnName)
			throws NoSuchColumnException {
		Column col = cols.get(columnName);
		if (col != null)
			return col;
		else 
			throw new NoSuchColumnException();
		
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getColumns(java.lang.String[])
	 */
	@Override
	public Operator<Column> getColumns(String... columnNames)
			throws NoSuchColumnException {				
		
		MyOperator<Column> opCol = new MyOperator<Column>();
		for (String tmp : columnNames) {
			opCol.addDataElement(cols.get(tmp));
		}
		
		return opCol;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getRows()
	 */
	@Override
	public Operator<Row> getRows() {
		Operator<Row> opRow = new MyOperator<Row>(rows);
		return opRow;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getRows(systeminterface.PredicateTreeNode)
	 */
	@Override
	public Operator<Row> getRows(PredicateTreeNode predicate)
			throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getTableName()
	 */
	@Override
	public String getTableName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#renameColumn(java.lang.String, java.lang.String)
	 */
	@Override
	public void renameColumn(String oldColumnName, String newColumnName)
			throws ColumnAlreadyExistsException, NoSuchColumnException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#updateRow(int, systeminterface.Row)
	 */
	@Override
	public void updateRow(int tupleID, Row newRow)
			throws SchemaMismatchException, NoSuchRowException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#updateRow(systeminterface.Row, systeminterface.Row)
	 */
	@Override
	public void updateRow(Row oldRow, Row newRow)
			throws SchemaMismatchException, NoSuchRowException {
		// TODO Auto-generated method stub

	}

}
