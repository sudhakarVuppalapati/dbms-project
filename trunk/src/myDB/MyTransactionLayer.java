package myDB;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import logrecords.DeleteLogPayload;
import logrecords.InsertLogPayload;
import logrecords.LogPayload;
import logrecords.LogRecord;
import logrecords.UpdateLogPayload;
import operator.Operator;
import relationalalgebra.CrossProduct;
import relationalalgebra.Input;
import relationalalgebra.Join;
import relationalalgebra.Projection;
import relationalalgebra.RelationalAlgebraExpression;
import relationalalgebra.Selection;
import systeminterface.IndexLayer;
import systeminterface.LogStore;
import systeminterface.QueryLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import systeminterface.TransactionLayer;
import util.LoggedOperation;
import util.RelationalOperatorType;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.NoSuchTransactionException;
import exceptions.SchemaMismatchException;
import exceptions.TableLockedException;

/**
 * Transaction Layer. Implement this skeleton. You have to use the constructor
 * shown here.
 * 
 */
public class MyTransactionLayer implements TransactionLayer {

	private final StorageLayer storageLayer;

	private final IndexLayer indexLayer;

	private final QueryLayer queryLayer;

	private LogStore logStore;

	/**
	 * TODO: Alternatives: using map to maintain list of transaction. Need
	 * experiments
	 */
	private TransactionMap trans = new TransactionMap();

	/** Hash table is used to guarantee synchronization */
	private Hashtable<String, Integer> lockedTables = new Hashtable<String, Integer>();

	/**
	 * Please do not modify this constructor.
	 * 
	 * @param storageLayer
	 *            A reference to the underlying storage layer
	 * @param indexLayer
	 *            A reference to the underlying index layer
	 * @param queryLayer
	 *            A reference to the underlying query layer
	 */
	public MyTransactionLayer(StorageLayer storageLayer, IndexLayer indexLayer,
			QueryLayer queryLayer) {
		this.storageLayer = storageLayer;
		this.indexLayer = indexLayer;
		this.queryLayer = queryLayer;
	}

	@Override
	public boolean abortTransaction(int TID) throws NoSuchTransactionException {

		if (!trans.isActive(TID))
			throw new NoSuchTransactionException("Cannot abort transaction "
					+ TID);

		// Write log ahead
		LogRecord logRecord = new LogRecord(TID,
				LoggedOperation.ABORT_TRANSACTION, null);
		logStore.writeRecord(logRecord);

		// Roll back non-committed changes
		LogRecord[] logs = trans.getOperations(TID);
		LogPayload payload;
		Row tmpRow;
		MyTable t;
		int rowID;
		try {
			for (int i = trans.getOpNo(TID) - 1; i >= 0; i--) {
				payload = logs[i].getLogPayload();
				t = (MyTable) storageLayer.getTableByName(payload
						.getTableName());
				rowID = payload.getRowID();

				if (logs[i].getOperation() == LoggedOperation.DELETE) {
					tmpRow = ((DeleteLogPayload) payload).getDeletedRow();
					t.insertRow(rowID, tmpRow);
				} else if (logs[i].getOperation() == LoggedOperation.INSERT) {
					t.deleteRow(rowID);
				} else if (logs[i].getOperation() == LoggedOperation.UPDATE) {
					tmpRow = ((UpdateLogPayload) payload).getOldRow();
					t.updateRow(rowID, tmpRow);
				}
			}
		} catch (NoSuchTableException ex) {
			ex.printStackTrace();
		} catch (NoSuchRowException ex) {
			ex.printStackTrace();
		} catch (SchemaMismatchException ex) {
			ex.printStackTrace();
		}
		// Release locks for all tables. Note that enumerations of hashtable are
		// not fail-fast

		String table;

		synchronized (lockedTables) {
			Enumeration<String> keys = lockedTables.keys();

			while (keys.hasMoreElements()) {
				table = keys.nextElement();
				if (lockedTables.get(table).intValue() == TID) {
					lockedTables.remove(table);
				}
			}
		}

		trans.clearTransaction(TID);

		// might be unnecessary
		return true;
	}

	@Override
	public int beginTransaction() {
		// Write log ahead
		int transID = trans.newTransaction();
		LogRecord log = new LogRecord(transID,
				LoggedOperation.START_TRANSACTION, null);
		logStore.writeRecord(log);
		return transID;
	}

	@Override
	public boolean commitTransaction(int TID) throws NoSuchTransactionException {

		if (trans.isActive(TID)) {
			// Write log ahead
			LogRecord log = new LogRecord(TID,
					LoggedOperation.COMMIT_TRANSACTION, null);
			logStore.writeRecord(log);

			// Release locks for all tables. Note that enumerations of hashtable
			// are not fail-fast

			String table;

			synchronized (lockedTables) {
				Enumeration<String> keys = lockedTables.keys();
				List<String> list = new ArrayList<String>();

				while (keys.hasMoreElements()) {
					table = keys.nextElement();
					if (lockedTables.get(table).intValue() == TID) {
						list.add(table);
					}
				}
				for (String str : list)
					lockedTables.remove(str);
			}

			trans.clearTransaction(TID);

			return true;
		} else
			throw new NoSuchTransactionException(
					"Cannot commit. Invalid transaction ID: " + TID);
	}

	@Override
	public void deleteRow(int TID, String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException, NoSuchTransactionException,
			TableLockedException {

		if (trans.isActive(TID)) {
			Integer in = lockedTables.get(tableName);
			if (in == null) {
				lockedTables.put(tableName, TID);
			} else if (in.intValue() != TID)
				throw new TableLockedException();

			MyTable t = ((MyTable) storageLayer.getTableByName(tableName));
			int[] rowIDs = t.getRowID(row);

			if (rowIDs.length == 0)
				throw new NoSuchRowException();

			for (int i = 0; i < rowIDs.length; i++) {
				LogRecord log = new LogRecord(TID, LoggedOperation.DELETE,
						new DeleteLogPayload(rowIDs[i], tableName, row));
				trans.writeOperation(TID, log);
				logStore.writeRecord(log);

				// delete row
				t.deleteRow(rowIDs[i]);

			}
			// Write log ahead
		} else
			throw new NoSuchTransactionException(
					"Cannot delete. Invalid transaction ID: " + TID);
	}

	@Override
	public String explain(int TID, RelationalAlgebraExpression query)
			throws NoSuchTableException, SchemaMismatchException,
			NoSuchColumnException, InvalidPredicateException,
			NoSuchTransactionException, TableLockedException {

		if (!trans.isActive(TID))
			throw new NoSuchTransactionException(
					"Cannot explain query for transaction: " + TID);

		Hashtable<String, Object> tempTables = new Hashtable<String, Object>();
		String table;

		synchronized (lockedTables) {
			if (testLocks(query, TID, tempTables)) {
				Enumeration<String> keys = tempTables.keys();
				while ((table = keys.nextElement()) != null) {
					if (!lockedTables.contains(table)) {
						lockedTables.put(table, TID);
					}
					// OOp, it's very bad
					else if (lockedTables.get(table).intValue() != TID)
						throw new TableLockedException();
				}
			}

			// This might be erroneous
			else
				throw new TableLockedException();
		}
		// Get explain from query layer
		return queryLayer.explain(query);
	}

	@Override
	public void insertRow(int TID, String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException,
			NoSuchTransactionException, TableLockedException {

		if (!trans.isActive(TID))
			throw new NoSuchTransactionException(
					"Cannot insert row. Invalid transaction ID: " + TID);

		Integer i = lockedTables.get(tableName);
		if (i == null) {
			lockedTables.put(tableName, TID);
		} else if (i.intValue() != TID)
			throw new TableLockedException();

		// TODO might need to check for row existence
		MyTable t = ((MyTable) storageLayer.getTableByName(tableName));
		int rowID = t.getAllRowCount();

		// Write log ahead
		LogRecord log = new LogRecord(TID, LoggedOperation.INSERT,
				new InsertLogPayload(rowID, tableName, row));
		trans.writeOperation(TID, log);
		logStore.writeRecord(log);

		// insert row
		t.addRow(row);
	}

	@Override
	public Operator<? extends Row> query(int TID,
			RelationalAlgebraExpression query) throws NoSuchTableException,
			SchemaMismatchException, NoSuchColumnException,
			InvalidPredicateException, NoSuchTransactionException,
			TableLockedException {

		if (!trans.isActive(TID))
			throw new NoSuchTransactionException(
					"Cannot perform query for transaction: " + TID);

		Hashtable<String, Object> tempTables = new Hashtable<String, Object>();
		String table;

		synchronized (lockedTables) {
			if (testLocks(query, TID, tempTables)) {
				Enumeration<String> keys = tempTables.keys();
				while (keys.hasMoreElements()) {
					table = keys.nextElement();
					if (!lockedTables.contains(table)) {
						lockedTables.put(table, TID);
					}
					// OOp, it's very bad
					else if (lockedTables.get(table).intValue() != TID)
						throw new TableLockedException();
				}
			}

			// This might be erroneous
			else
				throw new TableLockedException();
		}
		// Get explain from query layer
		return queryLayer.query(query);
	}

	@Override
	public void updateRow(int TID, String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, SchemaMismatchException,
			NoSuchTableException, NoSuchTransactionException,
			TableLockedException {

		if (!trans.isActive(TID))
			throw new NoSuchTransactionException(
					"Cannot update row. Invalid transaction ID: " + TID);

		Integer i = lockedTables.get(tableName);
		if (i == null) {
			lockedTables.put(tableName, TID);
		} else if (i.intValue() != TID)
			throw new TableLockedException();

		// TODO might need to check for row existence
		MyTable t = ((MyTable) storageLayer.getTableByName(tableName));
		int[] rowIDs = t.getRowID(oldRow);

		if (rowIDs.length == 0)
			throw new NoSuchRowException();

		for (int j = 0; j < rowIDs.length; j++) {
			LogRecord log = new LogRecord(TID, LoggedOperation.UPDATE,
					new UpdateLogPayload(rowIDs[j], tableName, oldRow, newRow));
			trans.writeOperation(TID, log);
			logStore.writeRecord(log);

			// insert row
			t.updateRow(rowIDs[j], newRow);
		}
		// Write log ahead
	}

	@Override
	public void setLogStore(LogStore logStore) {
		this.logStore = logStore;
	}

	private boolean testLocks(RelationalAlgebraExpression query, int TID,
			Map pushTables) {
		RelationalOperatorType type = query.getType();

		if (type == RelationalOperatorType.INPUT) {
			String tableName = ((Input) query).getRelationName();
			Integer transID = lockedTables.get(tableName);
			if (transID == null) {
				if (!pushTables.containsKey(tableName))
					pushTables.put(tableName, tableName);
				return true;
			} else
				return (transID.intValue() == TID);
		}

		else if (type == RelationalOperatorType.CROSS_PRODUCT) {
			return testLocks(((CrossProduct) query).getLeftInput(), TID,
					pushTables)
					& testLocks(((CrossProduct) query).getRightInput(), TID,
							pushTables);
		} else if (type == RelationalOperatorType.SELECTION) {
			return testLocks(((Selection) query).getInput(), TID, pushTables);
		} else if (type == RelationalOperatorType.PROJECTION) {
			return testLocks(((Projection) query).getInput(), TID, pushTables);
		} else if (type == RelationalOperatorType.JOIN) {
			return testLocks(((Join) query).getLeftInput(), TID, pushTables)
					& testLocks(((Join) query).getRightInput(), TID, pushTables);
		} else
			return true;
	}

	private final class TransactionMap {

		/**
		 * Keep track of transactions. Might be sparse if too many are committed
		 */
		private LogRecord[][] transactions;

		/** Number of active transactions */
		private int transNo;

		/** Number of operations per transaction */
		private int[] opNo;

		TransactionMap() {
			// Default capacity: 10 initial transaction
			transactions = new LogRecord[10][];
			opNo = new int[10];
			transNo = 0;
		}

		synchronized int newTransaction() {
			if (transNo == transactions.length) {
				// Default load factor: 1.5f
				LogRecord[][] newTrans = new LogRecord[(transNo * 3) / 2 + 1][];
				int[] newOpNo = new int[(transNo * 3) / 2 + 1];
				System.arraycopy(transactions, 0, newTrans, 0, transNo);
				System.arraycopy(opNo, 0, newOpNo, 0, transNo);
				opNo = newOpNo;
				newOpNo = null; // enable garbage-collection
				transactions = newTrans;
				newTrans = null; // enable garbage-collection
			}
			// Default operations number: 5
			transactions[transNo] = new LogRecord[5];
			// logStore.writeRecord(new LogRecord(transNo,
			// LoggedOperation.START_TRANSACTION, null));
			return transNo++;
		}

		void writeOperation(int TID, LogRecord opLog) {
			int n = transactions[TID].length;
			if (opNo[TID] == n) {
				LogRecord[] newLogs = new LogRecord[(n * 3) / 2 + 1];
				System.arraycopy(transactions[TID], 0, newLogs, 0, n);
				transactions[TID] = newLogs;
				newLogs = null;
			}
			transactions[TID][opNo[TID]++] = opLog;
		}

		LogRecord[] getOperations(int TID) {
			return transactions[TID];
		}

		boolean isActive(int TID) {
			return (transactions[TID] != null);
		}

		void clearTransaction(int TID) {
			transactions[TID] = null;
		}

		int getOpNo(int TID) {
			return opNo[TID];
		}
	}
}
