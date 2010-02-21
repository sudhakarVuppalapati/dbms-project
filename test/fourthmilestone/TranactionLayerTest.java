package fourthmilestone;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import junit.framework.TestCase;
import logrecords.InsertLogPayload;
import logrecords.LogRecord;
import logrecords.UpdateLogPayload;
import metadata.Type;
import metadata.Types;
import myDB.MyLogStore;
import operator.Operator;
import relationalalgebra.Input;
import sampleDB.ColumnInfo;
import sampleDB.SampleRow;
import systeminterface.Database;
import systeminterface.Row;
import util.LoggedOperation;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.NoSuchTransactionException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;
import exceptions.TableLockedException;

/**
 * Test cases for the transaction layer interface.
 * 
 */

public class TranactionLayerTest extends TestCase {

	private Database myDatabase;

	// Table names
	String S1 = "S1";

	// Schemas
	HashMap<String, Type> S1_Schema = new HashMap<String, Type>();

	// Column Names
	// S1
	String sid1 = "sid1";

	String sname1 = "sname1";

	String rating1 = "rating1";

	String age1 = "age1";

	// Schemas
	List<ColumnInfo> schema_S1;

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		// Files do not survive between tests
		// After test is done, remove all persisted tables for next tests
		File directory = new File("disk/");

		// Get all files in directory

		File[] files = directory.listFiles();
		for (File file : files) {

			System.out.println(file.getName());

			if (!file.delete()) {

				System.out.println("Failed to delete " + file);
			}
		}

	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		myDatabase = Database.getInstance();

		myDatabase.startSystem();

		// table schemas

		S1_Schema.put(sid1, Types.getIntegerType());
		S1_Schema.put(sname1, Types.getVarcharType());
		S1_Schema.put(rating1, Types.getIntegerType());
		S1_Schema.put(age1, Types.getIntegerType());

		schema_S1 = new ArrayList<ColumnInfo>();

		ColumnInfo S1_Col1 = new ColumnInfo(sid1, S1_Schema.get(sid1));
		ColumnInfo S1_Col2 = new ColumnInfo(sname1, S1_Schema.get(sname1));
		ColumnInfo S1_Col3 = new ColumnInfo(rating1, S1_Schema.get(rating1));
		ColumnInfo S1_Col4 = new ColumnInfo(age1, S1_Schema.get(age1));

		schema_S1.add(S1_Col1);
		schema_S1.add(S1_Col2);
		schema_S1.add(S1_Col3);
		schema_S1.add(S1_Col4);


		// table creation

		try {
			myDatabase.getQueryInterface().createTable(S1, S1_Schema);
		} catch (TableAlreadyExistsException e) {
			fail("TableAlreadyExistsException");
		}

		// Allows writing table schema to disk.
		myDatabase.shutdownSystem();

	}

	/**
	 * Test that attempts to access a locked table result in a
	 * TableLockedException
	 */
	public void testLocking() {

		MyLogStore logStore = new MyLogStore();

		myDatabase = Database.getInstance();
		myDatabase.startSystem(logStore);

		// Start transactions
		int TA1 = myDatabase.getTransactionInterface().beginTransaction();
		int TA2 = myDatabase.getTransactionInterface().beginTransaction();

		// This causes TA1 to lock table S1
		try {
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}

		// Now, TA2 attempts to access table S1 while TA1 has an exclusive lock
		// on it.

		try {
			myDatabase.getTransactionInterface().insertRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));
			fail("Expected TableLockedException");
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			// Expected to come here
		}

		myDatabase.shutdownSystem();

	}

	/**
	 * Test that a transaction undoes any changes it makes to the database upon
	 * aborting
	 */
	public void testTransactionAbort() {

		MyLogStore logStore = new MyLogStore();

		myDatabase = Database.getInstance();
		myDatabase.startSystem(logStore);

		// Start TA1. TA1 is nice, it will commit
		int TA1 = myDatabase.getTransactionInterface().beginTransaction();

		// Perform insertions
		try {
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))));
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}

		// Commit TA1
		try {
			myDatabase.getTransactionInterface().commitTransaction(TA1);
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		}

		// Start TA2. TA2 is mean, it will abort
		int TA2 = myDatabase.getTransactionInterface().beginTransaction();

		try {

			// add a row
			myDatabase.getTransactionInterface().insertRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 44,
							"guppy", 5, 35 }))));

			// delete a row
			myDatabase.getTransactionInterface().deleteRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));

			// update a row, be careful how you undo this
			myDatabase.getTransactionInterface().updateRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))),
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

			// delete 2 (now) identical rows
			myDatabase.getTransactionInterface().deleteRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		} catch (NoSuchRowException e) {
			fail("NoSuchRowException");
		}

		// Abort TA2
		try {
			myDatabase.getTransactionInterface().abortTransaction(TA2);
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		}

		// Start TA3. TA3 will just read table S1
		int TA3 = myDatabase.getTransactionInterface().beginTransaction();

		// query table s1

		TreeSet<String> expectedResultSet = new TreeSet<String>();
		TreeSet<String> resultSet = new TreeSet<String>();

		expectedResultSet.add("Dustin");
		expectedResultSet.add("Rusty");
		expectedResultSet.add("Lubber");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getTransactionInterface().query(TA3,
					new Input(S1));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null)

		{
			count++;

			try {
				resultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();
		}

		rowOp.close();

		assertEquals(3, count);
		assertTrue(resultSet.equals(expectedResultSet));

		myDatabase.shutdownSystem();
	}

	
	
	/**
	 * 
	 * Test recovery after a crash.
	 */
	public void testRecovery(){
		

		MyLogStore logStore = new MyLogStore();

		myDatabase = Database.getInstance();
		myDatabase.startSystem(logStore);
		
		// Start TA1.
		int TA1 = myDatabase.getTransactionInterface().beginTransaction();

		
		// Perform insertions
		try {
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))));
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));
			myDatabase.getTransactionInterface().insertRow(
					TA1,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}
		
		// Commit TA1
		try {
			myDatabase.getTransactionInterface().commitTransaction(TA1);
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		}

		// Start TA2. TA2 is mean, it will abort
		int TA2 = myDatabase.getTransactionInterface().beginTransaction();

		try {

			// add a row
			myDatabase.getTransactionInterface().insertRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 44,
							"guppy", 5, 35 }))));

			// delete a row
			myDatabase.getTransactionInterface().deleteRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));

			// update a row, be careful how you undo this
			myDatabase.getTransactionInterface().updateRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))),
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

			// delete 2 (now) identical rows
			myDatabase.getTransactionInterface().deleteRow(
					TA2,
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));

		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		} catch (NoSuchRowException e) {
			fail("NoSuchRowException");
		}

		
		//crash database, TA1 has committed, TA2 was not
		myDatabase.crash();
		
		//restart database after crash
		myDatabase = Database.getInstance();
		myDatabase.startSystem(logStore);
		
		// Start TA3. TA3 will just read table S1
		int TA3 = myDatabase.getTransactionInterface().beginTransaction();

		// query table s1

		TreeSet<String> expectedResultSet = new TreeSet<String>();
		TreeSet<String> resultSet = new TreeSet<String>();

		expectedResultSet.add("Dustin");
		expectedResultSet.add("Rusty");
		expectedResultSet.add("Lubber");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getTransactionInterface().query(TA3,
					new Input(S1));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null)

		{
			count++;

			try {
				resultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();
		}

		rowOp.close();

		assertEquals(3, count);
		assertTrue(resultSet.equals(expectedResultSet));

		
		myDatabase.shutdownSystem();
		
	}

	/**
	 * Here, you perform recovery based on an existing log store.
	 */
	public void testRecoveryUsingExternalLogStore() {

		myDatabase = Database.getInstance();
		myDatabase.startSystem();

		int rowId2;
		rowId2 = 0;

		//Add two rows using the storage layer interface.
		try {
			myDatabase.getStorageInterface().getTableByName(S1).addRow(
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))));
			rowId2 = myDatabase.getStorageInterface().getTableByName(S1)
					.addRow(
							(Row) ((new SampleRow(schema_S1, new Object[] { 22,
									"Dustin", 7, 45 }))));
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		}

		myDatabase.shutdownSystem();

		// Log store will say that the second row inserted (see above), had been
		// updated in a committed TA, TA1. TA2 is noise, it was never committed.

		int TA1 = 1;
		int TA2 = 2;

		MyLogStore logStore = new MyLogStore();

		logStore.writeRecord(new LogRecord(TA1,
				LoggedOperation.START_TRANSACTION, null));

		logStore.writeRecord(new LogRecord(TA1, LoggedOperation.UPDATE,
				new UpdateLogPayload(rowId2, S1, (Row) ((new SampleRow(
						schema_S1, new Object[] { 22, "Dustin", 7, 45 }))),
						(Row) ((new SampleRow(schema_S1, new Object[] { 44,
								"guppy", 5, 35 }))))));

		logStore.writeRecord(new LogRecord(TA2,
				LoggedOperation.START_TRANSACTION, null));

		logStore.writeRecord(new LogRecord(TA1,
				LoggedOperation.COMMIT_TRANSACTION, null));

		logStore.writeRecord(new LogRecord(TA2, LoggedOperation.INSERT,
				new InsertLogPayload(222, S1, (Row) ((new SampleRow(schema_S1,
						new Object[] { 31, "Lubber", 8, 55 }))))));


		// Start the database, now recovery should be performed.
		myDatabase = Database.getInstance();
		myDatabase.startSystem(logStore);

		// Start TA for reading table S1 after recovery
		int TA3 = myDatabase.getTransactionInterface().beginTransaction();

		// query table s1
		TreeSet<String> expectedResultSet = new TreeSet<String>();
		TreeSet<String> resultSet = new TreeSet<String>();

		expectedResultSet.add("Dustin");
		expectedResultSet.add("guppy");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getTransactionInterface().query(TA3,
					new Input(S1));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		} catch (NoSuchTransactionException e) {
			fail("NoSuchTransactionException");
		} catch (TableLockedException e) {
			fail("TableLockedException");
		}

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null)

		{
			count++;

			try {
				resultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();
		}

		rowOp.close();

		assertEquals(2, count);
		assertTrue(resultSet.equals(expectedResultSet));

		myDatabase.shutdownSystem();

	}
}
