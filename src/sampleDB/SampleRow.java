package sampleDB;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import metadata.Type;
import systeminterface.Row;
import exceptions.NoSuchColumnException;

/**
 * 
 * 
 */
public class SampleRow implements Row {

	private final Object[] rowContents;
	private final List<ColumnInfo> rowSchema;
	private final Map<String, Integer> position;

	/**
	 * @param rowSchema
	 *            schema of row
	 * @param data
	 *            data of row in same order as schema
	 */
	public SampleRow(List<ColumnInfo> rowSchema, Object[] data) {
		if (rowSchema.size() != data.length) {
			throw new IllegalArgumentException();
		}
		this.position = new HashMap<String, Integer>();
		this.rowSchema = rowSchema;
		rowContents = data;
		for (int i = 0; i < rowSchema.size(); ++i) {
			position.put(rowSchema.get(i).getName(), i);
		}
	}

	@Override
	public int getColumnCount() {
		// TODO Auto-generated method stub
		return rowContents.length;
	}

	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		Integer index = position.get(columnName);
		if (index != null) {
			return rowSchema.get(index).getType();
		}
		throw new NoSuchColumnException();
	}

	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException {
		Integer pos = position.get(columnName);

		// System.out.println(pos);

		// MIGHT NEED TO CHECK THIS (was pos-1)

		if (pos != null) {
			return rowContents[pos];
		}
		throw new NoSuchColumnException();
	}

	@Override
	public String[] getColumnNames() {

		String columnNames[] = new String[this.rowSchema.size()];

		for (int i = 0; i < this.rowSchema.size(); i++) {

			columnNames[i] = this.rowSchema.get(i).getName();
		}

		return columnNames;
	}

}
