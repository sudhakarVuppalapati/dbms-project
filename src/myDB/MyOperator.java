package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import operator.Operator;

public class MyOperator<OUTPUT> implements Operator<OUTPUT> {

	private Collection<OUTPUT> data;
	
	private Iterator<OUTPUT> it;
	
	public MyOperator() {
		data = new ArrayList<OUTPUT>();
	}
	
	public MyOperator(Collection<OUTPUT> dataValue) {
		data = dataValue;
	}
	
	public void addDataElement(OUTPUT dataEl){
		data.add(dataEl);
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		it = null;
	}

	@Override
	public OUTPUT next() {

		// TODO Auto-generated method stub
		if(it.hasNext()) {
			return it.next();
		}
		else 
			return null;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		it= data.iterator();
	}

}
