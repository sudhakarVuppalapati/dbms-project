package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyDoubleColumn extends MyColumn {
	
	private double[] data;
	private byte[] statuses;
	private int curSize;
	
	public MyDoubleColumn(String name, Type type){
		super(name, type);
		data= new double[this.defaulInitialCapacity];
		statuses=new byte[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyDoubleColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new double[Math.round(initialCapacity*FACTOR)];
		statuses=new byte[Math.round(initialCapacity*FACTOR)];
		curSize=0;
	}
	
	public MyDoubleColumn(String name,Type type,double[] data){
		super(name,type);
		this.data=data;
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public Double getElement(int rowID) throws NoSuchRowException {
		try{
			return new Double(data[rowID]);
		}
		catch(NullPointerException npe){
			throw new NoSuchRowException();
		}
	}

	@Override
	public int getRowCount() {
		return curSize;
	}
	
	@Override
	public void add(Object newData){
		//check if there is place for a new value
		if(curSize == data.length){
			//if not, allocate a new array
			double[] data1=new double[Math.round(FACTOR*curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data=data1;
			data=null; // try to force garbage collection
		}
		
		//add the new value
		data[curSize++]=((Double)newData).doubleValue();
	}

}
