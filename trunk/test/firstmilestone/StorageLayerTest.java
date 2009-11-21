package firstmilestone;

import java.io.File;
import java.util.HashMap;

import junit.framework.TestCase;
import metadata.Type;
import metadata.Types;
import operator.Operator;
import systeminterface.Database;
import systeminterface.Table;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * 
 * Test storage layer, lots of repetition for clarity
 * 
 * Note that the singleton Database is persisted between tests, so you have to
 * make sure you remove all tables between tests
 * 
 * @author Mohamed
 * 
 */
public class StorageLayerTest extends TestCase {

	private static final String tName = "TestTable1";

	/**
	 * @param name
	 *            name
	 */
	public StorageLayerTest(String name) {
		super(name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#tearDown()
	 */
	@SuppressWarnings("unchecked")
	protected void tearDown() throws Exception {

		super.tearDown();

		// DB is a singleton, remove all tables between runs
		Database myDatabase = Database.getInstance();

		myDatabase.startSystem();

		Operator<Table> op = (Operator<Table>) myDatabase.getStorageInterface()
				.getTables();

		op.open();

		Table t;

		while ((t = op.next()) != null) {

			myDatabase.getStorageInterface().deleteTable(t.getTableName());
		}

		myDatabase.shutdownSystem();

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

	/**
     * 
     */
	@SuppressWarnings("unchecked")
	public void testPersistence() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();

		tableSchema.put("attribute1", Types.getCharType(5));
		tableSchema.put("attribute2", Types.getDateType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("Table creation failed");

		}

		// shut down db (persist)
		myDatabase.shutdownSystem();

		// could completely shutdown application at this point (will be tested)

		// start system again
		myDatabase.startSystem();

		// get all tables stored
		Operator<Table> tablesOp = (Operator<Table>) myDatabase
				.getStorageInterface().getTables();

		tablesOp.open();
		Table t;

		for (int i = 0; (t = tablesOp.next()) != null; i++) {

			assertEquals(tName, t.getTableName());

			// one table inserted
			assertEquals(i, 0);
		}

		tablesOp.close();

	}

	/**
	 * Tests that no two tables with same name allowed
	 */

	public void testTableNameDuplication() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();

		// start DB
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("Table creation failed");
		}

		try { // create table with duplicate name
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
			fail("Should have received an exception when adding a duplicate table");

		} catch (TableAlreadyExistsException ex) { // this is what we want }

		}
	}

	/**
	 * Test deletion of tables
	 */

	public void testTableDeletion() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("col1", Types.getCharType(10));
		tableSchema.put("col2", Types.getDateType());

		// start DB
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		try {
			myDatabase.getStorageInterface().getTableByName(tName);
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		// delete table instance
		try {
			myDatabase.getStorageInterface().deleteTable(tName);
		} catch (NoSuchTableException e) { // TODO Auto-generated catch block
			fail("Table should have been  deleted");

		}

		try {
			myDatabase.getStorageInterface().deleteTable(tName);
			fail("Should have received an exception when deleting non-existing table");

		} catch (NoSuchTableException ex) { // nice exception! }

		}
	}

	/**
	 * test renaming of tables
	 */
	public void testRenameTable() {

		String newTableName = "t2";

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();

		// start DB
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		// rename table
		try {
			myDatabase.getStorageInterface().renameTable(tName, newTableName);
		} catch (NoSuchTableException e) {
			fail("unexpected");
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		// get table name
		String newTableNameCheck = null;
		try {
			newTableNameCheck = myDatabase.getStorageInterface()
					.getTableByName(newTableName).getTableName();
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		assertEquals(newTableName, newTableNameCheck);

		// take back to original state for tear down
		try {
			myDatabase.getStorageInterface().renameTable(newTableName, tName);

		} catch (NoSuchTableException ex) { // nice exception! }
			fail("unexpected");
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");

		}

	}

	// .... your own tests go here

}
