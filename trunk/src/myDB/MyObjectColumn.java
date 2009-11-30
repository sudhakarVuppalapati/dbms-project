package myDB;

import java.util.ArrayList;
import java.util.List;

import metadata.Type;
import exceptions.NoSuchRowException;

public class MyObjectColumn extends MyColumn {
	
	private List data;
	
	public MyObjectColumn(String name, Type type) {
		super(name,type);
		data=new ArrayList(defaulInitialCapacity);
	}
	
	public MyObjectColumn(String name, Type type, int initialCapacity) {
		super(name,type);
		data=new ArrayList(Math.round(initialCapacity*FACTOR));
	}
	
	public MyObjectColumn(String name, Type type,List data) {
		super(name,type);
		this.data=data;
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data.toArray();
	}

	@Override
	public Object getElement(int rowID) throws NoSuchRowException {
		try{
			return data.get(rowID);
		}
		catch(NullPointerException npe){
			throw new NoSuchRowException();
		}
	}

	@Override
	public int getRowCount() {
		return data.size();
	}

	@Override
	public void add(Object newObject) {
		data.add(newObject);
	}

}
