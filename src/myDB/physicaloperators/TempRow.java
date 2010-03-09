package myDB.physicaloperators;

import java.util.Map;

import metadata.Type;
import exceptions.NoSuchColumnException;
import systeminterface.Row;

public class TempRow implements Row {

	private Map<String, Type> schema;

	private Map<String, Object> data;

	public TempRow(Map<String, Type> schema, Map<String, Object> data) {
		this.schema = schema;
		this.data = data;
	}

	@Override
	public int getColumnCount() {
		return data.values().size();
	}

	@Override
	public String[] getColumnNames() {
		return schema.keySet().toArray(new String[0]);
	}

	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		return schema.get(columnName);
	}

	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException {
		return data.get(columnName);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		String names[] = schema.keySet().toArray(new String[0]);

		for (int i = 0; i < names.length; i++) {
			try {
				str.append(getColumnValue(names[i].toString()));
			} catch (NoSuchColumnException ex) {
				ex.printStackTrace();
			}
			str.append("/t");
		}
		return str.toString();
	}

}
