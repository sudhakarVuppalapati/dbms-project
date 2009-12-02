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
	private MyTable table;
	private int rowNo;
	
	
	private byte status; /* 0-unchanged, 1-deleted, 2-updated, 3-newly inserted*/
	
	public MyRow(Map<String,Type> tableSchema,Table table,int rowNumber){
		schema=((MyTable)table).getTableSchema();
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
		if(c==null){
			throw new NoSuchColumnException();
		}
		
		
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! take this out from here!!!!!
		Object data=c.getDataArrayAsObject();
		Type t=c.getColumnType();
		
		if(t == Types.getIntegerType()){
			//return new Integer(((int[])data)[rowNo]);
			int val=((int[])data)[rowNo];
			if(val==Integer.MIN_VALUE)
				return null;
			
			return val;
		}
		
		if(t == Types.getDoubleType()){
			//return new Double(((double[])data)[rowNo]);
			double val=((double[])data)[rowNo];
			if(val==Double.MIN_VALUE)
				return null;
			
			return val;
		}
		
		if(t == Types.getFloatType()){
			float val=((float[])data)[rowNo];
			if(val==Float.MIN_VALUE)
				return null;
			
			return val;
		}
		
		if(t == Types.getLongType()){
			long val=((long[])data)[rowNo];
			if(val==Long.MIN_VALUE)
				return null;
			
			return val;
		}
		
		return ((Object[])data)[rowNo];
		//return ((Object[])(.getDataArrayAsObject()))[rowNo];
	}
	
	
	
}
