package sampleDB.operators;

import java.util.Iterator;
import java.util.Vector;

import operator.Operator;

/**
 * Own implementation of op
 * 
 * @author myahya
 * 
 * @param <OUTPUT>
 */
public class SampleOperator<OUTPUT> implements Operator<OUTPUT> {

	Vector<OUTPUT> operatorContetnts = new Vector<OUTPUT>();
	Iterator<OUTPUT> it;

	/**
	 * @param element
	 */
	public void add(OUTPUT element) {

		operatorContetnts.add(element);

	}

	@Override
	public void close() {

	}

	@Override
	public OUTPUT next() {

		if (this.it.hasNext()) {

			return this.it.next();
		} else {

			return null;
		}

	}

	@Override
	public void open() {
		this.it = operatorContetnts.iterator();

	}

}
