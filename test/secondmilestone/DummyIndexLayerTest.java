package secondmilestone;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeSet;

import junit.framework.TestCase;
import metadata.Type;
import metadata.Types;
import operator.Operator;
import systeminterface.Database;
import systeminterface.Table;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * Test cases for the index layer interface. The tests here are for methods that
 * can be tested without other layers. Indexes will be manipulated here without
 * the underlying tables. Please make sure that your dropIndex is working
 * correctly, tests assume that.
 * 
 * @version r216
 */
public class DummyIndexLayerTest extends TestCase {

	private Database myDatabase = Database.getInstance();

	private final String tName = "table1";

	private final String att1 = "attribute1";

	private final String att2 = "attribute2";

	private final String att3 = "attribute3";

	private final String index1 = "index1";

	private final String index2 = "index2";

	private final String index3 = "index3";

	private boolean index1Created, index2Created, index3Created;

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		myDatabase.startSystem();

		Operator<? extends Table> op = myDatabase.getStorageInterface()
				.getTables();

		op.open();

		Table t;

		while ((t = op.next()) != null) {

			myDatabase.getStorageInterface().deleteTable(t.getTableName());
		}

		/*
		 * Drop all CREATED indexes.
		 */
		if (index1Created)
			myDatabase.getIndexInterface().dropIndex(index1);
		if (index2Created)
			myDatabase.getIndexInterface().dropIndex(index2);
		if (index3Created)
			myDatabase.getIndexInterface().dropIndex(index3);

		myDatabase.shutdownSystem();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		index1Created = index2Created = index3Created = false;

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getCharType(5));
		tableSchema.put(att2, Types.getDateType());
		tableSchema.put(att3, Types.getIntegerType());

		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

	}

	/**
	 * point queries
	 */
	public void testPointQueryRowID() {

		// System.out.println(myDatabase.getIndexInterface()==null);

		/*
		 * Create index
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att3,
					false);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/** Add entries to index * */
		ArrayList<Integer> valuesList = new ArrayList<Integer>();
		valuesList.add(5);
		valuesList.add(6);
		valuesList.add(43);

		try {

			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new Integer(20), valuesList.get(1));
			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new Integer(34), valuesList.get(2));
			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new Integer(12), valuesList.get(0));

		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		int rowIDs[] = null;
		TreeSet<Integer> expectedResultSet = new TreeSet<Integer>(valuesList);

		/** First query* */

		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new Integer(20));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		} 

		assertTrue(rowIDs != null);

		System.out.println("Result size= " + rowIDs.length);

		for (int i = 0; i < rowIDs.length; i++) {
			System.out.println(rowIDs[i]);
		}

		/** Second query* */
		rowIDs = null;
		expectedResultSet = new TreeSet<Integer>();
		expectedResultSet.add(5);
		expectedResultSet.add(6);

		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new Integer(12));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		} 

		assertTrue(rowIDs != null);

		System.out.println("Result size= " + rowIDs.length);

		for (int i = 0; i < rowIDs.length; i++) {
			System.out.println(rowIDs[i]);
		}

		/** Third query * */

		rowIDs = null;


		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new Integer(34));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		} 

		assertTrue(rowIDs != null);

		System.out.println("Result size= " + rowIDs.length);

		for (int i = 0; i < rowIDs.length; i++) {
			System.out.println(rowIDs[i]);
		}
	}

	/**
	 * Test deletion of entries from index
	 */
	public void testEntryDeletion() {

		/*
		 * Create index
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					false);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Insert into index
		 */

		try {

			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new String("DBDBD"), 1);
			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new String("DBDBD"), 2);
		} catch (NoSuchIndexException e) {
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Perform deletion
		 */

		try {
			myDatabase.getIndexInterface().deleteFromIndex(index1,
					new String("DBDBD"), 2);
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Get entries for "DB"
		 */

		int rowIDs[] = null;
		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new String("DBDBD"));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(rowIDs != null);
		assertTrue(rowIDs.length == 1);
		assertTrue(rowIDs[0] == 1);

	}

	/**
	 * Test deletion of all entries corresponding to a key
	 */
	public void testAllEntriesDeletion() {

		/*
		 * Create index
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					false);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Insert into index
		 */

		try {

			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new String("DBDBD"), 1);
			myDatabase.getIndexInterface().insertIntoIndex(index1,
					new String("DBDBD"), 2);
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Perform deletion
		 */

		try {
			myDatabase.getIndexInterface().deleteFromIndex(index1,
					new String("DBDBD"));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Get entries for "DB"
		 */

		int rowIDs[] = null;
		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new String("DBDBD"));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(rowIDs != null);
		assertTrue(rowIDs.length == 0);

	}

	/**
	 * Test updating an index entry
	 */
	public void testUpdateEntry() {

		/*
		 * Create index
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att2,
					false);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Insert into index
		 */

		try {

			myDatabase.getIndexInterface().insertIntoIndex(index1, new Date(0),
					1);
			myDatabase.getIndexInterface().insertIntoIndex(index1, new Date(0),
					2);
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Perform update
		 */

		try {
			myDatabase.getIndexInterface().updateIndex(index1, new Date(0), 1,
					11);
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		/*
		 * Check
		 */

		int rowIDs[] = null;
		TreeSet<Integer> resultSet = new TreeSet<Integer>();
		TreeSet<Integer> expectedResultSet = new TreeSet<Integer>();
		expectedResultSet.add(2);
		expectedResultSet.add(11);

		try {
			rowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(index1,
					new Date(0));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(rowIDs != null);

		System.out.println("Result size= " + rowIDs.length);

		for (int i = 0; i < rowIDs.length; i++) {
			resultSet.add(rowIDs[i]);
		}

		assertTrue(expectedResultSet.equals(resultSet));

	}

	/**
	 * Test for find index
	 */
	public void testFindIndex() {

		/*
		 * Create two indexes on att3, no indexes on att 1
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att3,
					false);
			myDatabase.getIndexInterface().createIndex(index2, tName, att3,
					false);
			index1Created = true;
			index2Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Find indexes on att3
		 */
		String foundIndexes[] = null;
		TreeSet<String> resultSet = new TreeSet<String>();
		TreeSet<String> expectedResultSet = new TreeSet<String>();
		expectedResultSet.add(index1);
		expectedResultSet.add(index2);

		try {
			foundIndexes = myDatabase.getIndexInterface()
					.findIndex(tName, att3);
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		assertTrue(foundIndexes != null);

		for (int i = 0; i < foundIndexes.length; i++) {
			resultSet.add(foundIndexes[i]);
		}

		assertTrue(expectedResultSet.equals(resultSet));

		/*
		 * Find indexes on att1
		 */

		try {
			assertTrue(myDatabase.getIndexInterface().findIndex(tName, att1) == null);
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}
	}

	/*
	 * Your tests go here
	 */

}
