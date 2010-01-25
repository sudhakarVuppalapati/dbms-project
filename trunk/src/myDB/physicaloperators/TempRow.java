package myDB.physicaloperators;

import metadata.Type;
import exceptions.NoSuchColumnException;
import systeminterface.Row;

public class TempRow implements Row {

	@Override
	public int getColumnCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getColumnNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException {
		// TODO Auto-generated method stub
		return null;
	}

}
