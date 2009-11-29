package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

public class MyDoubleColumn extends MyColumn {
	
	private double[] data;
	private int curSize;
	
	public MyDoubleColumn(String name, Type type){
		super(name, type);
		data= new double[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyDoubleColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new double[initialCapacity];
		curSize=0;
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
		return data.length;
	}
	
	@Override
	public void addData(Object newData){
		if(curSize == data.length){
			System.arraycopy(data, 0, data1, 0, length)
		}
	}

}
