package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import operator.Operator;

public class MyOperator<OUTPUT> implements Operator<OUTPUT> {

	private Collection<OUTPUT> data = new ArrayList<OUTPUT>();
	
	private Iterator<OUTPUT> it;
	
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
