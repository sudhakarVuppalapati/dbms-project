/**
 * 
 */
package logrecords;

import systeminterface.Row;

/**
 * Payload for logging a deletion.
 * 
 */
public class DeleteLogPayload extends LogPayload {

	private final Row deletedRow;

	/**
	 * @param rowID
	 * @param tableName
	 * @param deletedRow
	 */
	public DeleteLogPayload(int rowID, String tableName, Row deletedRow) {
		super(rowID, tableName);
		this.deletedRow = deletedRow;

	}

	/**
	 * Get deleted row corresponding to this log record.
	 * 
	 * @return row.
	 */
	public Row getDeletedRow() {

		return this.deletedRow;
	}

}
