package logrecords;

/**
 * Payload of a log record, where appropriate
 * 
 */

public abstract class LogPayload {

	private final int rowID;
	private final String tableName;

	/**
	 * 
	 * Contr
	 * 
	 * @param rowID
	 * @param tableName
	 */
	public LogPayload(int rowID, String tableName) {

		this.rowID = rowID;
		this.tableName = tableName;
	}

	/**
	 * Get the row ID for the row affected by the logged operation.
	 * 
	 * @return row ID
	 */
	public int getRowID() {
		return rowID;
	}

	/**
	 * Get table name for the table affected by the logged operation
	 * 
	 * @return table name
	 */
	public String getTableName() {
		return tableName;
	}
}
