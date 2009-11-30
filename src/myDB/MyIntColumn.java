package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyIntColumn extends MyColumn {
	
	private int[] data;
	private byte[] statuses;
	private int curSize;
	
	public MyIntColumn(String name, Type type) {
		super(name,type);
		data= new int[this.defaulInitialCapacity];
		statuses=new byte[this.defaulInitialCapacity];
		curSize=0;
	}
	
	public MyIntColumn(String name, Type type,int initialCapacity) {
		super(name,type);
		data = new int[Math.round(initialCapacity*FACTOR)];
		statuses=new byte[Math.round(initialCapacity*FACTOR)];
		curSize=0;
	}
	
	public MyIntColumn(String name,Type type,int[] data){
		super(name,type);
		this.data=data;
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public Integer getElement(int rowID) throws NoSuchRowException {
		try{
			return new Integer(data[rowID]);
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
			int[] data1=new int[Math.round(FACTOR*curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data=data1;
			data=null; // try to force garbage collection
		}
		
		//add the new value
		data[curSize++]=((Integer)newData).intValue();
	}
	
	
}
