/**
 * 
 */
package myDB;

import metadata.*;
import exceptions.NoSuchRowException;
import systeminterface.Column;
import util.ComparisonOperator;

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
	protected static final float FACTOR= 1.2f;
	
	protected static final int defaulInitialCapacity=20;
	
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
	
	protected static final boolean check(Object obj1, Object obj2, Type type, ComparisonOperator op) {
		
		if (op == ComparisonOperator.EQ) {
			if (obj1 == null && obj2 == null) return true;
			else if (obj1.equals(obj2)) return true; //Re-Check with Date type
		}
		else if (op == ComparisonOperator.GEQ) {
			if (obj1 != null && obj2 != null) {
				if (type == Types.getIntegerType())
					if (((Integer)obj1).intValue() >= ((Integer)obj2).intValue()) return true;
					else return false;
				else if (type == Types.getFloatType())
					if (((Float)obj1).floatValue() >= ((Float)obj2).floatValue()) return true;
					else return false;
				else if (type == Types.getDoubleType())
					if (((Double)obj1).doubleValue() >= ((Double)obj2).doubleValue()) return true;
					else return false;
				else if (type == Types.getLongType())
					if (((Long)obj1).longValue() >= ((Long)obj2).longValue()) return true;
					else return false;
				else {
					try {
						if (((Comparable)obj1).compareTo(obj1) >= 0) return true;
						else return false;
					} catch (ClassCastException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
		}
		else if (op == ComparisonOperator.GT) {
			if (obj1 != null && obj2 != null) {
				if (type == Types.getIntegerType())
					if (((Integer)obj1).intValue() > ((Integer)obj2).intValue()) return true;
					else return false;
				else if (type == Types.getFloatType())
					if (((Float)obj1).floatValue() > ((Float)obj2).floatValue()) return true;
					else return false;
				else if (type == Types.getDoubleType())
					if (((Double)obj1).doubleValue() > ((Double)obj2).doubleValue()) return true;
					else return false;
				else if (type == Types.getLongType())
					if (((Long)obj1).longValue() > ((Long)obj2).longValue()) return true;
					else return false;
				else {
					try {
						if (((Comparable)obj1).compareTo(obj1) > 0) return true;
						else return false;
					} catch (ClassCastException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
		}
		else if (op == ComparisonOperator.LEQ) {
			if (obj1 != null && obj2 != null) {
				if (type == Types.getIntegerType())
					if (((Integer)obj1).intValue() <= ((Integer)obj2).intValue()) return true;
					else return false;
				else if (type == Types.getFloatType())
					if (((Float)obj1).floatValue() <= ((Float)obj2).floatValue()) return true;
					else return false;
				else if (type == Types.getDoubleType())
					if (((Double)obj1).doubleValue() <= ((Double)obj2).doubleValue()) return true;
					else return false;
				else if (type == Types.getLongType())
					if (((Long)obj1).longValue() <= ((Long)obj2).longValue()) return true;
					else return false;
				else {
					try {
						if (((Comparable)obj1).compareTo(obj1) <= 0) return true;
						else return false;
					} catch (ClassCastException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
		}
		else if (op == ComparisonOperator.LT) {
			if (obj1 != null && obj2 != null) {
				if (type == Types.getIntegerType())
					if (((Integer)obj1).intValue() < ((Integer)obj2).intValue()) return true;
					else return false;
				else if (type == Types.getFloatType())
					if (((Float)obj1).floatValue() < ((Float)obj2).floatValue()) return true;
					else return false;
				else if (type == Types.getDoubleType())
					if (((Double)obj1).doubleValue() < ((Double)obj2).doubleValue()) return true;
					else return false;
				else if (type == Types.getLongType())
					if (((Long)obj1).longValue() < ((Long)obj2).longValue()) return true;
					else return false;
				else {
					try {
						if (((Comparable)obj1).compareTo(obj1) < 0) return true;
						else return false;
					} catch (ClassCastException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
		}
		else if (op == ComparisonOperator.NEQ) {
			if (obj1 != null && obj2 != null) {
				if (type == Types.getIntegerType())
					if (((Integer)obj1).intValue() != ((Integer)obj2).intValue()) return true;
					else return false;
				else if (type == Types.getFloatType())
					if (((Float)obj1).floatValue() != ((Float)obj2).floatValue()) return true;
					else return false;
				else if (type == Types.getDoubleType())
					if (((Double)obj1).doubleValue() != ((Double)obj2).doubleValue()) return true;
					else return false;
				else if (type == Types.getLongType())
					if (((Long)obj1).longValue() != ((Long)obj2).longValue()) return true;
					else return false;
				else {
					try {
						if (((Comparable)obj1).compareTo(obj1) != 0) return true;
						else return false;
					} catch (ClassCastException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
		}
		return false;
	}
	
	
	//temporary method just to end with the third milestone
	public abstract void eraseOldArray();

}
