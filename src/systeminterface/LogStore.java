/**
 * 
 */
package systeminterface;

import logrecords.LogRecord;
import operator.Operator;

/**
 * Log store interface. Think of this as a very fast disk used for writing log
 * records.
 * 
 */
public interface LogStore {

	/**
	 * Write a record to the log store.
	 * 
	 * @param record
	 *            The record to write to the log.
	 */
	public void writeRecord(LogRecord record);

	/**
	 * Get all log records in the log store. Log records for a single
	 * transaction should be returned in that same order as the operations that
	 * resulted in the generation of these records.
	 * 
	 * @return Operator with log records
	 */
	public Operator<? extends LogRecord> getLogRecords();

	/**
	 * Clear all records in this log store.
	 */
	public void clear();

}
