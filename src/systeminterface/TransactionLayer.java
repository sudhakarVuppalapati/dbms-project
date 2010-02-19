/**
 * 
 */
package systeminterface;

import operator.Operator;
import relationalalgebra.RelationalAlgebraExpression;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.NoSuchTransactionException;
import exceptions.SchemaMismatchException;
import exceptions.TableLockedException;

/**
 * @author myahya
 * 
 */
public interface TransactionLayer {

	/**
	 * Start a new transaction. A transaction is represented by an unique
	 * integer ID.
	 * 
	 * @return A unique ID for the transaction
	 */
	public int beginTransaction();

	/**
	 * 
	 * Set the log store that will be used by the transaction layer for logging.
	 * 
	 * @param logStore
	 *            The log store used for storing log records.
	 */
	public void setLogStore(LogStore logStore);

	/**
	 * End the transaction, committing all changes.
	 * 
	 * @param TID
	 *            The ID of the transaction to be committed
	 * @return true if the transaction committed successfully, false otherwise.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists.
	 */
	public boolean commitTransaction(int TID) throws NoSuchTransactionException;

	/**
	 * End the transaction, rolling back all changes made during the
	 * transaction.
	 * 
	 * @param TID
	 *            The ID of the transaction to be aborted
	 * @return true if the transaction aborted successfully, false otherwise.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists.
	 */
	public boolean abortTransaction(int TID) throws NoSuchTransactionException;

	/**
	 * Insert a new Row.
	 * 
	 * @param TID
	 *            Transaction ID of the transaction in which this operation will
	 *            be performed.
	 * @param tableName
	 *            Name of the table where row is inserted.
	 * @param row
	 *            The row to be inserted
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists.
	 * @throws TableLockedException
	 *             This operation is trying to access a table that is locked by
	 *             another transaction.
	 * 
	 */
	public void insertRow(int TID, String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException,
			NoSuchTransactionException, TableLockedException;

	/**
	 * Delete a row completely matching the supplied one.
	 * 
	 * @param TID
	 *            Transaction ID of the transaction in which this operation will
	 *            be performed.
	 * @param tableName
	 *            Name of the table where row is inserted.
	 * @param row
	 *            The row to be deleted.
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 * @throws NoSuchRowException
	 *             No row matching the supplied one exists in the table.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists.
	 * @throws TableLockedException
	 *             This operation is trying to access a table that is locked by
	 *             another transaction.
	 */
	public void deleteRow(int TID, String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException, NoSuchTransactionException,
			TableLockedException;

	/**
	 * Update the contents of a row.
	 * 
	 * @param TID
	 *            Transaction ID of the transaction in which this operation will
	 *            be performed.
	 * @param tableName
	 *            Name of the table where row is updated.
	 * @param oldRow
	 *            The row to be replaced.
	 * @param newRow
	 *            Replacement row.
	 * @throws SchemaMismatchException
	 *             The schema of the supplied row does not match that of the
	 *             table.
	 * @throws NoSuchRowException
	 *             No row matching the supplied old row exists in the table.
	 * @throws NoSuchTableException
	 *             Supplied table does not exist.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists. *
	 * @throws TableLockedException
	 *             This operation is trying to access a table that is locked by
	 *             another transaction.
	 */
	public void updateRow(int TID, String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, SchemaMismatchException,
			NoSuchTableException, NoSuchTransactionException,
			TableLockedException;

	/**
	 * 
	 * Perform a query.
	 * 
	 * @param TID
	 *            Transaction ID of the transaction in which this operation will
	 *            be performed.
	 * @param query
	 *            The root operator of the logical operator tree.
	 * @return An operator with the result of the query.
	 * @throws NoSuchTableException
	 *             A referenced table does not exist.
	 * @throws SchemaMismatchException
	 *             An issue dues to schema mismatch.
	 * @throws NoSuchColumnException
	 *             An attribute referenced in a table does not exist.
	 * @throws InvalidPredicateException
	 *             A predicate in a selection is not valid.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists. *
	 * @throws TableLockedException
	 *             This operation is trying to access a table that is locked by
	 *             another transaction.
	 */
	public Operator<? extends Row> query(int TID,
			RelationalAlgebraExpression query) throws NoSuchTableException,
			SchemaMismatchException, NoSuchColumnException,
			InvalidPredicateException, NoSuchTransactionException,
			TableLockedException;

	/**
	 * Give the physical execution plan of the query and any estimates. The
	 * format of the returned string and the exact information it contains is up
	 * to you, we will not strictly test it, but it should be meaningful.
	 * 
	 * @param TID
	 *            Transaction ID of the transaction in which this operation will
	 *            be performed.
	 * @param query
	 *            The root operator of the logical operator tree.
	 * @return An explanation
	 * @throws NoSuchTableException
	 *             A referenced table does not exist.
	 * @throws SchemaMismatchException
	 *             An issue dues to schema mismatch.
	 * @throws NoSuchColumnException
	 *             An attribute referenced in a table does not exist.
	 * @throws InvalidPredicateException
	 *             A predicate in a selection is not valid.
	 * @throws NoSuchTransactionException
	 *             No transaction with the given TID has been exists. *
	 * @throws TableLockedException
	 *             This operation is trying to access a table that is locked by
	 *             another transaction.
	 */
	public String explain(int TID, RelationalAlgebraExpression query)
			throws NoSuchTableException, SchemaMismatchException,
			NoSuchColumnException, InvalidPredicateException,
			NoSuchTransactionException, TableLockedException;

}
