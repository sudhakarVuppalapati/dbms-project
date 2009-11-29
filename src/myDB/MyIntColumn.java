package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

public class MyIntColumn extends MyColumn {
	
	private int[] data;
	
	public MyIntColumn(String name, Type type) {
		super(name,type);
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
		return data.length;
	}

}
