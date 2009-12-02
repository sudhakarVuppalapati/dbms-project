/**
 * 
 */
package myDB;

import metadata.*;
import exceptions.NoSuchRowException;
import systeminterface.Column;

/**
 * @author razvan
 */
public abstract class MyColumn implements Column {

	/* (non-Javadoc)
	 * @see systeminterface.Column#getColumnName()
	 */
	
	/**/
	private String name;
	
	private Type type;
	
	//syncronize with Const !!!!!!!!!
	protected final float FACTOR= 1.2f;
	
	protected final int defaulInitialCapacity=20;
	
	//private byte status;  /* 0-unchanged, 1-deleted, 2-updated, 3-newly added to schema*/
	
	public MyColumn(){
	}
	
	public MyColumn(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * @param name
	 * @param type
	 */
	/*public MyColumn(String name, Type type) {
		super();
		this.name = name;
		this.type = type;
		this.data = new ArrayList();
	}*/

	/**
	 * @param name
	 * @param type
	 * @param data
	 */
	/*public MyColumn(String name, Type type, List data) {
		super();
		this.name = name;
		this.type = type;
		this.data = data;
	}*/
	
	@Override
	public String getColumnName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getColumnType()
	 */
	@Override
	public Type getColumnType() {
		return type;
	}

	

	/* (non-Javadoc)
	 * @see systeminterface.Column#getElement(int)
	 */
	/*@Override
	public Object getElement(int rowID) throws NoSuchRowException {
		return data.get(rowID);
	}*/

	public abstract Object getElement(int rowID) throws NoSuchRowException;
	
	
	/* (non-Javadoc)
	 * @see systeminterface.Column#getRowCount()
	 */
	@Override
	/*public int getRowCount() {
		return data.size();
	}*/
	
	public abstract int getRowCount();

	/* (non-Javadoc)
	 * @see systeminterface.Column#setColumnName(java.lang.String)
	 */
	@Override
	public void setColumnName(String columnName) {
		name = columnName;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Column#getDataArrayAsObject()
	 */
	/*@Override
	public Object getDataArrayAsObject() {
		Class c=type.getClass();
		int size=data.size();
		int i;
		
		if(c == Types.getFloatType().getClass()){
			float[] d=new float[size];
			for(i=0; i<size;i++)
				d[i]=(Float)data.get(i);
			return d;
			
		}
		
		if(c == Types.getDoubleType().getClass()){
			double[] d=new double[size];
			for(i=0; i<size;i++)
				d[i]=(Double)data.get(i);
			return d;
			
		}
		
		if(c == Types.getIntegerType().getClass()){
			int[] d=new int[size];
			for(i=0; i<size;i++)
				d[i]=(Integer)data.get(i);
			return d;
			
		}
		
		if(c == Types.getLongType().getClass()){
			long[] d=new long[size];
			for(i=0; i<size;i++)
				d[i]=(Long)data.get(i);
			return d;
			
		}
		
		return data.toArray();
	}*/
	
	public abstract Object getDataArrayAsObject();
	
	public abstract void  setData(Object o,int curSize);
	
	public abstract void add(Object o);
	
	public abstract void remove(int rowID);
	
	public abstract void update(int rowID,Object value);
	
	//public abstract boolean isDeleted(int i);

}
