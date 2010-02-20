package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import operator.Operator;

public class MyOperator<OUTPUT> implements Operator<OUTPUT> {

	private Collection<OUTPUT> data;
	
	private Iterator<OUTPUT> it;
	
	private int curPos;
	
	public MyOperator() {
		data = new ArrayList<OUTPUT>();
	}

	public MyOperator(Collection<OUTPUT> dataValue) {
		data = dataValue;
	}

	@Override
	public void close() {
		it = null;
	}

	@Override
	public OUTPUT next() {
		//return it.next();
		if(curPos<data.size()){
			curPos++;
			return it.next();
		}
			
		return null;
		/*if(it.hasNext()) {
			return it.next();
		}
		else 
			return null;*/
	}

	@Override
	public void open() {
		it= data.iterator();
		curPos=0;
	}

}
