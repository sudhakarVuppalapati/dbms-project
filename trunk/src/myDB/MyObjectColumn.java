package myDB;

import java.util.ArrayList;
import java.util.List;
import metadata.Type;
import exceptions.NoSuchRowException;

/**
 * @author razvan
 */
public class MyObjectColumn extends MyColumn {
	
	//private List data;
	private Object[] data;
	//private byte statuses[];
	int curSize;
	
	public MyObjectColumn(String name, Type type) {
		super(name,type);
		//data=new ArrayList(defaulInitialCapacity);
		data = new Object[defaulInitialCapacity];
		//statuses=new byte[defaulInitialCapacity];
		curSize=0;
	}
	
	public MyObjectColumn(String name, Type type, int initialCapacity) {
		super(name,type);
		//data=new ArrayList(Math.round(initialCapacity*FACTOR));
		data = new Object[Math.round(initialCapacity * FACTOR)];
		//statuses=new byte[Math.round(initialCapacity*FACTOR)];
	}
	
	public MyObjectColumn(String name, Type type,/*List*/Object[] data) {
		super(name,type);
		this.data=data;
		//statuses=new byte[data.size()];
	}
	
	@Override
	public Object getDataArrayAsObject() {
		//return data.toArray();
		return data;
	}
	
	@Override
	/*public void setData(Object data,int curSize){
		this.data=(List)data;
		//statuses=new byte[this.data.size()];
		this.curSize=curSize;
	}*/
	public void setData(Object data,int curSize){
		this.data=(Object[])data;
		//statuses=new byte[this.data.size()];
		this.curSize=curSize;
	}

	
	@Override
	public Object getElement(int rowID) throws NoSuchRowException {
		if(rowID>=curSize /*|| statuses[rowID]==1*/)
			throw new NoSuchRowException();
		
		//return data.get(rowID);
		return data[rowID];
	}

	@Override
	public int getRowCount() {
		//return data.size();
		return curSize;
	}

	@Override
	/*public void add(Object newObject) {
		data.add(newObject);
		//Added by Tuan
		curSize++;
		
		//if(curSize == statuses.length){
			//the same for statuses
			//byte[] statuses1=new byte[Math.round(FACTOR*curSize)];
			//System.arraycopy(statuses, 0, statuses1, 0, curSize);
			//statuses=statuses1;
			//statuses1=null;
		//}
		//statuses[curSize++]=3;
	}
	*/
	public void add(Object newData){
		//check if there is place for a new value
		if(curSize == data.length){
			//if not, allocate a new array
			Object[] data1=new Object[Math.round(FACTOR*curSize)];
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
		data[curSize] = newData;
		curSize++;
		//statuses[curSize++]=3;
	}
	
	@Override
	public void remove(int rowID) {
		//statuses[rowID]=1;
		//data.set(rowID, MyNull.NULLOBJ);
		data[rowID] = MyNull.NULLOBJ;
	}
	
	@Override
	public void update(int rowID, Object value) {
		//data.set(rowID,value);
		//statuses[rowID]=2;
		data[rowID] = value;
	}
	
	@Override
	public void eraseOldArray(){
		this.data=null;
	}
	
}
