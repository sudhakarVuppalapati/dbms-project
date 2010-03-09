package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyLongColumn extends MyColumn {
	
	private long[] data;
	//private byte[] statuses;
	private int curSize;
	
	public MyLongColumn(String name, Type type) {
		super(name,type);
		data= new long[MyColumn.defaulInitialCapacity];
		//statuses=new byte[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyLongColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new long[Math.round(initialCapacity*FACTOR)];
		//statuses=new byte[Math.round(initialCapacity*FACTOR)];
		curSize=0;
	}
	
	public MyLongColumn(String name,Type type,long[] data){
		super(name,type);
		this.data=data;
		curSize=data.length;
		//statuses=new byte[curSize];
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public void setData(Object data,int curSize){
		this.data=(long[])data;
		//statuses=new byte[this.data.length];
		this.curSize=curSize;
	}
	
	@Override
	public Long getElement(int rowID) throws NoSuchRowException {
		if(rowID>=curSize /*|| statuses[rowID]==1*/)
			throw new NoSuchRowException();
		
		if(data[rowID] == Double.MIN_VALUE) 
			return null;
		
		return new Long(data[rowID]);
	}

	@Override
	public int getRowCount() {
		return curSize;
	}
	
	
	@Override
	public void add(Object newData){
		//check if there is place for a new value
		if(curSize == data.length){
			//if not, allocate a new array and copy everything form the old one
			long[] data1=new long[Math.round(FACTOR*curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data=data1;
			data1=null; // try to force garbage collection
			
			//the same for statuses
			//byte[] statuses1=new byte[Math.round(FACTOR*curSize)];
			//System.arraycopy(statuses, 0, statuses1, 0, curSize);
			//statuses=statuses1;
			//statuses1=null;
		}
		
		//add the new value
		data[curSize]=((Long)newData).longValue();
		curSize++;
		//statuses[curSize++]=3;
	}
	
	@Override
	public void remove(int rowID) {
		//statuses[rowID]=1;
		data[rowID]=Long.MAX_VALUE;
	}
	
	@Override
	public void update(int rowID, Object value) {
		if(value==null)
			data[rowID]=Long.MIN_VALUE;
		else 
			data[rowID]=((Long)value).longValue();
		//statuses[rowID]=2;
	}
	
	@Override
	public void eraseOldArray(){
		this.data=null;
	}
}
