package secondmilestone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;
import metadata.Type;
import metadata.Types;
import operator.Operator;

import org.junit.After;
import org.junit.Before;

import sampleDB.ColumnInfo;
import sampleDB.SampleRow;
import systeminterface.Database;
import systeminterface.Row;
import systeminterface.Table;
import exceptions.ColumnAlreadyExistsException;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * This is testing that operations performed on tables are also reflected in
 * indexes.
 */
public class QueryLayerTest extends TestCase {

	private Database myDatabase = Database.getInstance();

	private final String tName = "table1";

	private final String att1 = "attribute1";

	private final String att2 = "attribute2";

	private final String att3 = "attribute3";

	private final String index1 = "index1";

	private final String index2 = "index2";

	private final String index3 = "index3";

	private boolean index1Created, index2Created, index3Created;

	@Before
	public void setUp() throws Exception {

		index1Created = index2Created = index3Created = false;

		myDatabase.startSystem();

	}

	@After
	public void tearDown() throws Exception {

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

	/**
	 * When index is created, does it correctly reflect the underlying table.
	 * Think bulk-loading here
	 */
	public void testNewIndexReflectsTable() {

		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getIntegerType());

		// create 1 new table
		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		/*
		 * Load table
		 */
		Row row1, row2, row3;

		ColumnInfo ci = new ColumnInfo();
		ci.setName(att1);
		ci.setType(Types.getIntegerType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci);
		Object[] arr1 = new Object[1];
		Object[] arr2 = new Object[1];
		Object[] arr3 = new Object[1];

		// Might want to try out same key
		arr1[0] = new Integer(5);
		arr2[0] = new Integer(6);
		arr3[0] = new Integer(7);

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		int row1ID = 0, row2ID = 0, row3ID = 0;

		try {
			/*
			 * Here insertion not done through query layer because we want row
			 * IDs and no indexes exist yet.
			 */
			row1ID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row1);
			row2ID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row2);
			row3ID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row3);

		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Create index
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					true);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Get index entries and check them
		 */
		int result1RowIDs[] = null;

		try {
			result1RowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(
					index1, new Integer(5));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(result1RowIDs.length == 1);
		assertTrue(result1RowIDs[0] == row1ID);

		int result2RowIDs[] = null;

		try {
			result2RowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(
					index1, new Integer(6));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(result2RowIDs.length == 1);
		assertTrue(result2RowIDs[0] == row2ID);

		int result3RowIDs[] = null;

		try {
			result3RowIDs = myDatabase.getIndexInterface().pointQueryRowIDs(
					index1, new Integer(7));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}

		assertTrue(result3RowIDs.length == 1);
		assertTrue(result3RowIDs[0] == row3ID);

	}

	/**
	 * Test that inserting a row in a table is reflected in all corresponding
	 * indexes.
	 */
	public void testInsertReflectedInIndex() {

		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getIntegerType());

		// create 1 new table
		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected TableAlreadyExistsException");
		}

		/*
		 * Create an index on att1
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					true);
			index1Created = true;
		} catch (IndexAlreadyExistsException e1) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e1) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e1) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Load table
		 */

		Row row1, row2, row3;

		ColumnInfo ci = new ColumnInfo();
		ci.setName(att1);
		ci.setType(Types.getIntegerType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci);
		Object[] arr1 = new Object[1];
		Object[] arr2 = new Object[1];
		Object[] arr3 = new Object[1];

		Object[] arrOld = new Object[1];
		Object[] arrNew = new Object[1];

		arr1[0] = new Integer(5);
		arr2[0] = new Integer(5);
		arr3[0] = new Integer(7);

		arrOld[0] = new Integer(5);
		arrNew[0] = new Integer(50);

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		try {
			myDatabase.getQueryInterface().insertRow(tName, row1);
			myDatabase.getQueryInterface().insertRow(tName, row2);
			myDatabase.getQueryInterface().insertRow(tName, row3);

		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Check results in index
		 */
		Operator<? extends Row> rowOp = null;
		rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(5));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();
		int i = 0;
		for (i = 0; rowOp.next() != null; i++) {

		}
		rowOp.close();
		assertTrue(i == 2);

		rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(7));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();

		for (i = 0; rowOp.next() != null; i++) {

		}
		rowOp.close();
		assertTrue(i == 1);

	}

	/**
	 * When a column is renamed, this should be reflected in the index layer
	 */
	public void testColumnRenameReflectedInIndexes() {

		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getIntegerType());
		tableSchema.put(att2, Types.getVarcharType());

		// create 1 new table
		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected TableAlreadyExistsException");
		}

		/*
		 * Create an index on att2
		 */

		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att2,
					true);
			index1Created = true;
		} catch (IndexAlreadyExistsException e) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Rename a column
		 */

		try {
			myDatabase.getQueryInterface().renameColumn(tName, att2, att3);
		} catch (NoSuchTableException e) {
			fail("unexpected NoSuchTableException");
		} catch (ColumnAlreadyExistsException e) {
			fail("unexpected ColumnAlreadyExistsException");
		} catch (NoSuchColumnException e) {
			fail("unexpected NoSuchColumnException");
		}

		/*
		 * Lets try to find an index on old column Name
		 */

		try {
			myDatabase.getIndexInterface().findIndex(tName, att2);
		} catch (SchemaMismatchException e) {
			System.out.println("Nice!");
		} catch (NoSuchTableException e) {
			fail("unexpected NoSuchTableException");
		}

		/*
		 * Now, same thing, but for new column name
		 */
		String indexNames[] = null;
		try {
			indexNames = myDatabase.getIndexInterface().findIndex(tName, att3);
		} catch (SchemaMismatchException e) {
			System.out.println("Nice!");
		} catch (NoSuchTableException e) {
			fail("unexpected NoSuchTableException");
		}

		assertTrue(indexNames != null);
		assertTrue(indexNames.length == 1);
		assertTrue(indexNames[0].equals(index1));

	}

	/**
	 * Test that updating a row is reflected in all corresponding indexes.
	 */
	public void testUpdateRowReflectedInIndex() {

		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getIntegerType());

		// create 1 new table
		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected TableAlreadyExistsException");
		}

		/*
		 * Create an index on att1
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					true);
			index1Created = true;
		} catch (IndexAlreadyExistsException e1) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e1) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e1) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Load table
		 */

		Row row1, row2, row3, oldRow, newRow;

		ColumnInfo ci = new ColumnInfo();
		ci.setName(att1);
		ci.setType(Types.getIntegerType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci);
		Object[] arr1 = new Object[1];
		Object[] arr2 = new Object[1];
		Object[] arr3 = new Object[1];

		Object[] arrOld = new Object[1];
		Object[] arrNew = new Object[1];

		arr1[0] = new Integer(5);
		arr2[0] = new Integer(5);
		arr3[0] = new Integer(7);

		arrOld[0] = new Integer(5);
		arrNew[0] = new Integer(50);

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		oldRow = new SampleRow(schema, arrOld);
		newRow = new SampleRow(schema, arrNew);

		try {
			myDatabase.getQueryInterface().insertRow(tName, row1);
			myDatabase.getQueryInterface().insertRow(tName, row2);
			myDatabase.getQueryInterface().insertRow(tName, row3);

		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Perform Update
		 */
		try {
			myDatabase.getQueryInterface().updateRow(tName, oldRow, newRow);
		} catch (NoSuchRowException e) {
			fail("Unexpected NoSuchRowException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
			e.printStackTrace();
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Check results in index for pre-update key (should be gone)
		 */

		Operator<? extends Row> rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(5));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();

		int i = 0;
		for (i = 0; rowOp.next() != null; i++) {

		}
		rowOp.close();
		assertTrue(i == 0);

		/*
		 * Check results in index for post-update key (should be gone)
		 */

		rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(50));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();

		for (i = 0; rowOp.next() != null; i++) {

		}
		rowOp.close();
		assertTrue(i == 2);

	}

	/**
	 * Test that updating a row is reflected in all corresponding indexes.
	 */
	public void testDeleteRowReflectedInIndex() {

		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(att1, Types.getIntegerType());

		// create 1 new table
		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected TableAlreadyExistsException");
		}

		/*
		 * Create an index on att1
		 */
		try {
			myDatabase.getIndexInterface().createIndex(index1, tName, att1,
					true);
			index1Created = true;
		} catch (IndexAlreadyExistsException e1) {
			fail("Unexpected IndexAlreadyExistsException");
		} catch (SchemaMismatchException e1) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e1) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Load table
		 */

		Row row1, row2, row3, delRow;

		ColumnInfo ci = new ColumnInfo();
		ci.setName(att1);
		ci.setType(Types.getIntegerType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci);
		Object[] arr1 = new Object[1];
		Object[] arr2 = new Object[1];
		Object[] arr3 = new Object[1];

		Object[] arrDel = new Object[1];

		arr1[0] = new Integer(5);
		arr2[0] = new Integer(5);
		arr3[0] = new Integer(7);

		arrDel[0] = new Integer(7);

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		delRow = new SampleRow(schema, arrDel);

		try {
			myDatabase.getQueryInterface().insertRow(tName, row1);
			myDatabase.getQueryInterface().insertRow(tName, row2);
			myDatabase.getQueryInterface().insertRow(tName, row3);

		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Perform Deletion
		 */
		try {
			myDatabase.getQueryInterface().deleteRow(tName, delRow);
		} catch (NoSuchRowException e) {
			fail("Unexpected NoSuchRowException");
		} catch (SchemaMismatchException e) {
			fail("Unexpected SchemaMismatchException");
			e.printStackTrace();
		} catch (NoSuchTableException e) {
			fail("Unexpected NoSuchTableException");
		}

		/*
		 * Check results for deleted key
		 */

		Operator<? extends Row> rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(7));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();

		int i = 0;
		for (i = 0; rowOp.next() != null; i++) {

		}
		rowOp.close();
		assertTrue(i == 0);

		/*
		 * Check results in index for other key
		 */

		rowOp = null;
		try {
			rowOp = myDatabase.getIndexInterface().pointQuery(index1,
					new Integer(5));
		} catch (NoSuchIndexException e) {
			fail("Unexpected NoSuchIndexException");
		} catch (InvalidKeyException e) {
			fail("Unexpected InvalidKeyException");
		}
		rowOp.open();
		Row r = null;
		for (i = 0; (r = rowOp.next()) != null; i++) {
			try {
				assertTrue(r.getColumnValue(att1).equals(new Integer(5)));
			} catch (NoSuchColumnException e) {
				fail("Unexpected NoSuchColumnException");
			}
		}
		rowOp.close();
		assertTrue(i == 2);

	}

	/*
	 * You tests go here
	 */

}
