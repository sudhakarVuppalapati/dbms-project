/*
 * 
 */
package myDB;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import metadata.Type;
import metadata.Types;
import exceptions.NoSuchColumnException;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.Table;
import util.Pair;

/**
 * @author razvan
 *
 */
public class MyRow implements Row {

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnCount()
	 */
	
	private Map<String,Type> schema;
	private Table table;
	private int rowNo;
	
	//private byte status;
	
	public MyRow(Map<String,Type> tableSchema,Table table,int rowNumber){
		schema=tableSchema;
		rowNo=rowNumber;
	}
	
	/*public void addCell(String colName, Type type, Object value) {
		data.put(colName, new DataCell(type,value));
	}*/
	@Override
	public int getColumnCount() {
		return schema.size();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnNames()
	 */
	@Override
	public String[] getColumnNames() {
		// TODO Auto-generated method stub
		return (String[])schema.keySet().toArray();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnType(java.lang.String)
	 */
	@Override
	public Type getColumnType(String columnName) throws NoSuchColumnException {
		Type t=schema.get(columnName);
		if(t!=null)
			return t;
		throw new NoSuchColumnException();
	}

	/* (non-Javadoc)
	 * @see systeminterface.Row#getColumnValue(java.lang.String)
	 */
	@Override
	public Object getColumnValue(String columnName)
			throws NoSuchColumnException{
		
		Column c=table.getColumnByName(columnName);
		Object data=c.getDataArrayAsObject();
		Type t=c.getColumnType();
		
		if(t == Types.getIntegerType()){
			//return new Integer(((int[])data)[rowNo]);
			return ((int[])data)[rowNo];
		}
		
		if(t == Types.getDoubleType()){
			//return new Double(((double[])data)[rowNo]);
			return ((double[])data)[rowNo];
		}
		
		if(t == Types.getFloatType()){
			return ((float[])data)[rowNo];
		}
		
		if(t == Types.getLongType()){
			return ((long[])data)[rowNo];
		}
		
		return ((Object[])data)[rowNo];
		//return ((Object[])(.getDataArrayAsObject()))[rowNo];
	}

}
