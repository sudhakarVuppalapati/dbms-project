/**
 * 
 */
package logrecords;

import systeminterface.Row;

/**
 * Payload for logging an update.
 * 
 */
public class UpdateLogPayload extends LogPayload {

	private final Row oldRow;
	private final Row newRow;

	/**
	 * @param rowID
	 * @param tableName
	 * @param oldRow
	 * @param newRow
	 */
	public UpdateLogPayload(int rowID, String tableName, Row oldRow, Row newRow) {
		super(rowID, tableName);
		this.oldRow = oldRow;
		this.newRow = newRow;

	}

	/**
	 * Get deleted row corresponding to this log record.
	 * 
	 * @return row.
	 */
	public Row getOldRow() {

		return this.oldRow;
	}

	/**
	 * Get deleted row corresponding to this log record.
	 * 
	 * @return row.
	 */
	public Row getNewRow() {

		return this.newRow;
	}

}
