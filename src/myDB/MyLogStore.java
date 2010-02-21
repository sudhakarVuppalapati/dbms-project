/**
 * 
 */
package myDB;

import logrecords.LogRecord;
import operator.Operator;
import systeminterface.LogStore;

/**
 * 
 * Log Store. Implement this skeleton. You have to use the constructor shown
 * here.
 */
public class MyLogStore implements LogStore {

	/**
	 * Constructor. Please do not modify this constructor
	 */
	public MyLogStore() {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#clear()
	 */
	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#getLogRecords()
	 */
	@Override
	public Operator<? extends LogRecord> getLogRecords() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#writeRecord(logrecords.LogRecord)
	 */
	@Override
	public void writeRecord(LogRecord record) {
		// TODO Auto-generated method stub

	}

}
