package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyFloatColumn extends MyColumn {
	
	private float[] data;
	private byte[] statuses; /* 0-newly added, 1-deleted, 2-updated*/
	private int curSize;
	
	public MyFloatColumn(String name, Type type) {
		super(name, type);
		data= new float[this.defaulInitialCapacity];
		statuses=new byte[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyFloatColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new float[Math.round(initialCapacity*FACTOR)];
		statuses=new byte[Math.round(initialCapacity*FACTOR)];
		curSize=0;
	}
	
	public MyFloatColumn(String name,Type type,float[] data){
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
	public void setData(Object data,int curSize){
		this.data=(float[])data;
		statuses=new byte[this.data.length];
		this.curSize=curSize;
	}
	
	@Override
	public Float getElement(int rowID) throws NoSuchRowException {
		try{
			return new Float(data[rowID]);
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
			float[] data1=new float[Math.round(FACTOR*curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data=data1;
			data1=null; // try to force garbage collection
			
			//the same for statuses
			byte[] statuses1=new byte[Math.round(FACTOR*curSize)];
			System.arraycopy(statuses, 0, statuses1, 0, curSize);
			statuses=statuses1;
			statuses1=null;
		}
		
		//add the new value
		data[curSize]=((Float)newData).floatValue();
		statuses[curSize++]=3;
	}

	@Override
	public void remove(int rowID) {
		statuses[rowID]=1;
	}
	
	@Override
	public void update(int rowID, Object value) {
		data[rowID]=((Float)value).floatValue();
		statuses[rowID]=2;
	}

}
