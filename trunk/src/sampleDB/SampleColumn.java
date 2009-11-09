package sampleDB;

import java.util.Vector;

import metadata.Type;
import systeminterface.Column;

/**
 * @author myahya
 * 
 */
public class SampleColumn implements Column {

	private Vector<Object> columnContents = new Vector<Object>();

	private Type columnType = null;

	private String columnName;

	protected void addToColumn(Object element) {

		this.columnContents.add(element);

	}

	@Override
	public Type getColumnType() {
		// TODO Auto-generated method stub
		return this.columnType;
	}

	@Override
	public Object getDataArrayAsObject() throws Exception {
		return this.columnContents.toArray();
	}

	@Override
	public Object getElement(int rowNumber) {

		return this.columnContents.get(rowNumber);
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return this.columnName;
	}

	@Override
	public int getRowCount() {
		return this.columnContents.size();
	}

	public void setColumnName(String columnName) {

		this.columnName = columnName;
	}

	@Override
	public void setColumnType(Type columnType) {

		if (this.columnType == null) {
			this.columnType = columnType;
		}

	}

}
