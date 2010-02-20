package util;

/**
 * Possible update operations
 * 
 */
public enum LoggedOperation {

	/**
	 * Start a transaction
	 */
	START_TRANSACTION,

	/**
	 * Commit a transaction
	 */
	COMMIT_TRANSACTION,

	/**
	 * Abort a transaction
	 */
	ABORT_TRANSACTION,

	/**
	 * Insert operation
	 */
	INSERT,
	/**
	 * Delete operation
	 */
	DELETE,
	/**
	 * Update operation
	 */
	UPDATE

}
