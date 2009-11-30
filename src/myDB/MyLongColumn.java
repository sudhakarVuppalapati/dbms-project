package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyLongColumn extends MyColumn {
	
	private long[] data;
	private byte[] statuses;
	private int curSize;
	
	public MyLongColumn(String name, Type type) {
		super(name,type);
		data= new long[this.defaulInitialCapacity];
		statuses=new byte[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyLongColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new long[Math.round(initialCapacity*FACTOR)];
		statuses=new byte[Math.round(initialCapacity*FACTOR)];
		curSize=0;
	}
	
	public MyLongColumn(String name,Type type,long[] data){
		super(name,type);
		this.data=data;
		curSize=data.length;
		statuses=new byte[curSize];
		
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public Long getElement(int rowID) throws NoSuchRowException {
		try{
			return new Long(data[rowID]);
		}
		catch(NullPointerException npe){
			throw new NoSuchRowException();
		}
	}

	@Override
	public int getRowCount() {
		return curSize;
	}
	
	
	public void add(Object newData){
		//check if there is place for a new value
		if(curSize == data.length){
			//if not, allocate a new array and copy everything form the old one
			long[] data1=new long[Math.round(FACTOR*curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data=data1;
			data=null; // try to force garbage collection
		}
		
		//add the new value
		data[curSize++]=((Long)newData).longValue();
	}
	
	@Override
	public void remove(int rowID) {
		statuses[rowID]=1;
	}
	
	@Override
	public void update(int rowID, Object value) {
		data[rowID]=((Long)value).longValue();
		statuses[rowID]=2;
	}
}
