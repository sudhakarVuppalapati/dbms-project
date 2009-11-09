package sampleDB;

import java.util.HashMap;

import operator.Operator;
import systeminterface.Column;
import systeminterface.PersistentExtent;
import systeminterface.Row;
import systeminterface.Table;
import util.Pair;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;

/**
 * @author myahya
 * 
 */
public class SampleTable implements Table {

	// need to preserve order on columns
	private HashMap<String, Column> columnMap;

	private HashMap<Integer, String> columnOrder;

	private int totalNumColumns = 0;

	private int cardinality = 0;

	/**
	 * 
	 */
	public SampleTable() {
		this.columnMap = new HashMap<String, Column>();
		this.columnOrder = new HashMap<Integer, String>();

	}

	@Override
	public int addColumn(Column column) throws ColumnAlreadyExistsException {

		if (!this.columnMap.containsKey(column.getName())) {

			this.columnMap.put(column.getName(), column);
			this.totalNumColumns += 1;
			this.columnOrder.put(new Integer(this.totalNumColumns), column
					.getName());

		} else {

			throw new ColumnAlreadyExistsException();
		}

		// LOOK HERE
		return 1;

	}

	@Override
	public void addRow(Row row) {

		int rowDimension = row.getColumnCount();

		if (rowDimension != this.columnMap.size()) {
			System.err.println("Row does not match table schema");
		}

		for (int in = 0; in < this.columnOrder.size(); in++) {

			try {
				((SampleColumn) this.columnMap.get(this.columnOrder
						.get(new Integer(in + 1)))).addToColumn(row
						.getColumnValue(in + 1));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		cardinality += 1;

	}

	@Override
	public void assignExtent(PersistentExtent extent) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRow(Row row) {
		// TODO Auto-generated method stub
		cardinality -= 1;

	}

	@Override
	public void dropColumnByID(int columnID) throws NoSuchColumnException {
		// TODO Auto-generated method stub

	}

	@Override
	public Column getColumnByID(int columnID) throws NoSuchColumnException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getColumnIDByName(String columnName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Operator<Pair<Integer, Column>> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<Row> getRows() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {

		String output = "";

		for (int i = 0; i < this.cardinality; i++) {
			for (int in = 0; in < this.columnOrder.size(); in++) {

				output += this.columnMap.get(
						this.columnOrder.get(new Integer(in + 1)))
						.getElement(i).toString();

				output += "\t\t";

			}

			output += "\n";
		}

		return output;

	}

	@Override
	public void updateRow(Row oldRow, Row newRow) {
		// TODO Auto-generated method stub

	}

}
