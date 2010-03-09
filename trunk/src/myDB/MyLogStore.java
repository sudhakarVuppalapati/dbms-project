/**
 * 
 */
package myDB;

import java.util.ArrayList;
import java.util.List;

import logrecords.LogRecord;
import operator.Operator;
import systeminterface.LogStore;

/**
 * 
 * Log Store. Implement this skeleton. You have to use the constructor shown
 * here.
 */
public class MyLogStore implements LogStore {

	List<LogRecord> logs;

	/**
	 * Constructor. Please do not modify this constructor
	 */
	public MyLogStore() {
		logs = new ArrayList<LogRecord>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#clear()
	 */
	@Override
	public void clear() {
		logs.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#getLogRecords()
	 */
	@Override
	public Operator<? extends LogRecord> getLogRecords() {
		return new MyOperator<LogRecord>(logs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.LogStore#writeRecord(logrecords.LogRecord)
	 */
	@Override
	public void writeRecord(LogRecord record) {
		logs.add(record);
	}

}
