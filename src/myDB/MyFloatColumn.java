package myDB;

import metadata.Type;
import exceptions.NoSuchRowException;

public class MyFloatColumn extends MyColumn {
	
	private float[] data;
	
	public MyFloatColumn(String name, Type type) {
		super(name,type);
	}
	
	@Override
	public Object getDataArrayAsObject() {
		return data;
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
		return data.length;
	}

}
