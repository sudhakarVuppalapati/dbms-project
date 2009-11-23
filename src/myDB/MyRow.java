/**
 * 
 */
package myDB;

import java.util.HashMap;
import java.util.Map;
import metadata.Type;
import exceptions.NoSuchColumnException;
import systeminterface.Row;

/**
 * @author razvan
 *
 */
public class MyRow implements Row {

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnCount()
	 */
	
	private Map<String,DataCell> data;
	
	//private byte status;
	
	public MyRow(){
		data=new HashMap();
	}
	
	public void addCell(String colName, Type type, Object value) {
		data.put(colName, new DataCell(type,value));
	}
	@Override
	public int getColumnCount() {
		return data.size();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnNames()
	 */
	@Override
	public String[] getColumnNames() {
		// TODO Auto-generated method stub
		return (String[])data.keySet().toArray();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnType(java.lang.String)
	 */
	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		if(data.containsKey(columnName)){
			return data.get(columnName).getType();
		}
		throw new NoSuchColumnException();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnValue(java.lang.String)
	 */
	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException {
		if(data.containsKey(columnName)){
			return data.get(columnName).getValue();
		}
		throw new NoSuchColumnException();
	}

}
