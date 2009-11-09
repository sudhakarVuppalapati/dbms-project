package sampleDB;

import metadata.Type;
import systeminterface.Row;

/**
 * @author myahya
 * 
 */
public class SampleRow implements Row {

	Object[] rowContents;

	@Override
	public int getColumnCount() {
		// TODO Auto-generated method stub
		return rowContents.length;
	}

	@Override
	public Type getColumnType(int column) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getColumnValue(int column) throws Exception {

		return rowContents[column - 1];
	}

	/**
	 * @param elements
	 */
	public void initializeRow(Object[] elements) {

		this.rowContents = elements;

	}

}
