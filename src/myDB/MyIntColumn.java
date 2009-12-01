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
		curSize=data.length;
		statuses=new byte[curSize];
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public void setData(Object data,int curSize){
		this.data=(int[])data;
		statuses=new byte[this.data.length];
		this.curSize=curSize;
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
			data1=null; // try to force garbage collection
			
			//the same for statuses
			byte[] statuses1=new byte[Math.round(FACTOR*curSize)];
			System.arraycopy(statuses, 0, statuses1, 0, curSize);
			statuses=statuses1;
			statuses1=null;
		}
		
		//add the new value
		//System.out.println("New data:"+newData);
		data[curSize]=((Integer)newData).intValue();
		statuses[curSize++]=3;
	}
	
	@Override
	public void remove(int rowID) {
		statuses[rowID]=1;
	}
	
	@Override
	public void update(int rowID, Object value) {
		data[rowID]=((Integer)value).intValue();
		statuses[rowID]=2;
	}	

	@Override
	public boolean isDeleted(int i) {
		return (statuses[i] == 1);
	}
}
