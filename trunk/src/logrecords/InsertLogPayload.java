/**
 * 
 */
package logrecords;

import systeminterface.Row;

/**
 * Payload for logging an insertion.
 * 
 */
public class InsertLogPayload extends LogPayload {

	private final Row insertedRow;

	/**
	 * @param rowID
	 * @param tableName
	 * @param insertedRow
	 */
	public InsertLogPayload(int rowID, String tableName, Row insertedRow) {
		super(rowID, tableName);
		this.insertedRow = insertedRow;

	}

	/**
	 * Get inserted row corresponding to this log record.
	 * 
	 * @return row.
	 */
	public Row getInsertedRow() {

		return this.insertedRow;
	}

}
