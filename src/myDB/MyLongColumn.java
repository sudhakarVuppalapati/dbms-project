package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

public class MyLongColumn extends MyColumn {
	
	private long[] data;
	
	public MyLongColumn(String name, Type type) {
		super(name,type);
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
		return data.length;
	}

}
