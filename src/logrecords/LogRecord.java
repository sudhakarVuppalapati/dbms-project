/**
 * 
 */
package logrecords;

import util.LoggedOperation;

/**
 * A log record
 * 
 */
public class LogRecord {

	/*
	 * Fields
	 */

	private final int TID;
	private final LogPayload logPayload;
	private final LoggedOperation loggedOperation;

	/**
	 * 
	 * Contr for log record. Depending on the operation being logged, a record
	 * could also have a payload.
	 * 
	 * @param TID
	 * @param loggedOperation
	 *            The operation being logged
	 * @param logPayload
	 *            A payload where appropriate, null otherwise
	 * 
	 */
	public LogRecord(int TID, LoggedOperation loggedOperation,
			LogPayload logPayload) {

		if (loggedOperation == LoggedOperation.START_TRANSACTION
				|| loggedOperation == LoggedOperation.COMMIT_TRANSACTION
				|| loggedOperation == LoggedOperation.ABORT_TRANSACTION) {

			if (logPayload != null) {

				throw new IllegalArgumentException(
						"Payload should be null with TA start, abort or commit");
			}
		} else {

			if (logPayload == null) {

				throw new IllegalArgumentException(
						"Payload should may not be null for update operations");
			}
		}
		this.TID = TID;
		this.loggedOperation = loggedOperation;
		this.logPayload = logPayload;

	}

	/*
	 * Methods
	 */

	/**
	 * Get TID for this record
	 * 
	 * @return TID
	 */
	public int getTID() {

		return this.TID;
	}

	/**
	 * Get operation to which this log record corresponds.
	 * 
	 * @return Operation type
	 */
	public LoggedOperation getOperation() {

		return this.loggedOperation;
	}

	/**
	 * Get the payload corresponding to this log entry
	 * 
	 * @return Log payload
	 */
	public LogPayload getLogPayload() {

		return this.logPayload;
	}

}
