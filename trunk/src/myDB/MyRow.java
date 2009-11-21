/**
 * 
 */
package myDB;

import java.util.Map;

import metadata.Type;
import exceptions.NoSuchColumnException;
import systeminterface.Row;

/**
 * @author tuanta
 *
 */
public class MyRow implements Row {

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnCount()
	 */
	private Map<String, Object> cells;
	
	private Object data[colCount];
	
	@Override
	public int getColumnCount() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnNames()
	 */
	@Override
	public String[] getColumnNames() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnType(java.lang.String)
	 */
	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnValue(java.lang.String)
	 */
	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException {
		// TODO Auto-generated method stub
		return null;
	}

}
