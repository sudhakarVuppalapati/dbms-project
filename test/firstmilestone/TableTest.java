package firstmilestone;

import java.io.File;
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
import sampleDB.InnerPredicateTreeNode;
import sampleDB.LeafPredicateTreeNode;
import sampleDB.SampleRow;
import systeminterface.Column;
import systeminterface.Database;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.Table;
import util.ComparisonOperator;
import util.LogicalOperator;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * 
 * Test cases against the Table, Column and Row interface
 * 
 * You might want to make some changes to this test
 * 
 * @author Mohamed
 * 
 */
public class TableTest extends TestCase {

	private static final String tName = "TestTable1";

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

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
	 * test get all columns
	 */
	public void testGetAllColumns() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getCharType(5));
		tableSchema.put("attribute2", Types.getDateType());
		tableSchema.put("attribute3", Types.getIntegerType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Operator<Column> op = null;
		try {
			op = (Operator<Column>) (myDatabase.getStorageInterface()
					.getTableByName(tName)).getAllColumns();
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		op.open();
		Column c;
		int i;
		for (i = 0; (c = op.next()) != null; i++) {

			// same attribute name
			assertTrue(tableSchema.containsKey(c.getColumnName()));

			// and same type
			assertTrue(tableSchema.get(c.getColumnName()).getName().equals(
					c.getColumnType().getName()));

		}

		op.close();

		// same size -> no dups
		assertEquals(i, tableSchema.size());

	}

	/**
	 * Test whether returned column has correct info about itself
	 */
	public void testGetColumnByName() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getCharType(5));
		tableSchema.put("attribute2", Types.getDateType());
		tableSchema.put("attribute3", Types.getIntegerType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Column c = null;
		try {
			c = myDatabase.getStorageInterface().getTableByName(tName)
					.getColumnByName("attribute1");
		} catch (NoSuchColumnException e) {
			fail("unexpected");
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		assertEquals(c.getColumnName(), "attribute1");
		assertEquals(c.getColumnType(), Types.getCharType(5));
		assertEquals(c.getRowCount(), 0);

	}

	/**
	 * test columns being renamed correctly and references maintained
	 * 
	 * After getting reference to a column in my table, any modifications to the
	 * table should be reflected in the column OR you can decide to throw an
	 * unchecked runtime exception indicating that the column I have is no more
	 * valid.
	 * 
	 */
	public void testRenameColumn() {
		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getCharType(5));

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		// materialize column
		Column c1 = null;
		try {
			c1 = myDatabase.getStorageInterface().getTableByName(tName)
					.getColumnByName("attribute1");
		} catch (NoSuchColumnException e) {
			fail("unexpected");
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		// rename column
		try {
			myDatabase.getStorageInterface().getTableByName(tName)
					.renameColumn("attribute1", "attribute2");
		} catch (ColumnAlreadyExistsException e1) {
			fail("unexpected");
		} catch (NoSuchColumnException e1) {
			fail("unexpected");
		} catch (NoSuchTableException e1) {
			fail("unexpected");
		} catch (RuntimeException e1) {

		}

		assertEquals(c1.getColumnName(), "attribute2");
		assertEquals(c1.getColumnType(), Types.getCharType(5));
		assertEquals(c1.getRowCount(), 0);

		try {
			myDatabase.getStorageInterface().getTableByName(tName)
					.getColumnByName("attribute1");
			fail("should have reported NoSuchColumnException");
		} catch (NoSuchColumnException e) {

		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

	}

	/**
	 * 
	 */
	public void testGetRowsWithPredicate() {

		// get new database instance
		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getIntegerType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Row row1, row2, row3;

		ColumnInfo ci = new ColumnInfo();
		ci.setName("attribute1");
		ci.setType(Types.getIntegerType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci);
		Object[] arr1 = new Object[1];
		Object[] arr2 = new Object[1];
		Object[] arr3 = new Object[1];

		arr1[0] = new Integer(5);
		arr2[0] = new Integer(6);
		arr3[0] = new Integer(7);

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		try {
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row1);
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row2);
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row3);
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");

		}

		Operator<Row> rowOp = null;

		// change failure condition when changing this

		/*
		 * PredicateTreeNode predicate = new LeafPredicateTreeNode("attribute1",
		 * ComparisonOperator.GEQ, new Integer(6));
		 */

		PredicateTreeNode predicate = new InnerPredicateTreeNode(
				new LeafPredicateTreeNode("attribute1", ComparisonOperator.GEQ,
						new Integer(6)), LogicalOperator.AND,
				new LeafPredicateTreeNode("attribute1", ComparisonOperator.LEQ,
						new Integer(6)));

		try {

			rowOp = (Operator<Row>) myDatabase.getStorageInterface()
					.getTableByName(tName).getRows(predicate);

		} catch (NoSuchTableException e) {
			fail("unexpected");
		} catch (SchemaMismatchException e) {
			fail("unexpected");

		}

		rowOp.open();
		Row r;
		int i;
		for (i = 0; (r = rowOp.next()) != null; i++) {

			int val = 0;
			try {
				val = ((Integer) r.getColumnValue("attribute1")).intValue();
			} catch (NoSuchColumnException e) {
				fail("unexpected");

			}

			System.out.println(val);

			if (val != 6) {
				fail("Incorrect predicate eval");
			}

		}

		if (i != 1) {

			fail("empty result");
		}

	}

	/**
	 * Test parameters from methods for Column and Row
	 */
	public void testColumnRowParams() {

		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getIntegerType());
		tableSchema.put("attribute2", Types.getVarcharType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Row row1, row2, row3;

		ColumnInfo ci1 = new ColumnInfo();
		ColumnInfo ci2 = new ColumnInfo();

		ci1.setName("attribute1");
		ci1.setType(Types.getIntegerType());
		ci2.setName("attribute2");
		ci2.setType(Types.getVarcharType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci1);
		schema.add(ci2);
		Object[] arr1 = new Object[2];
		Object[] arr2 = new Object[2];
		Object[] arr3 = new Object[2];

		arr1[0] = new Integer(5);
		arr1[1] = "I am row 1";
		arr2[0] = new Integer(6);
		arr2[1] = "I am row 2";
		arr3[0] = new Integer(7);
		arr3[1] = "I am row 3";

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		int rowID = 0;
		try {
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row1);
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row2);
			rowID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row3);
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");
		}

		Column c = null;

		try {
			c = myDatabase.getStorageInterface().getTableByName(tName)
					.getColumnByName("attribute2");
		} catch (NoSuchColumnException e) {
			fail("unexpected");

		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		assertEquals(c.getColumnName(), "attribute2");
		try {
			assertEquals(c.getElement(rowID), "I am row 3");
		} catch (NoSuchRowException e1) {
			fail("unexpected");
		}
		assertEquals(c.getColumnType(), Types.getVarcharType());
		assertEquals(c.getRowCount(), 3);

		/*
		 * Now let's see rows
		 */

		Operator<Row> rowOp = null;

		try {
			rowOp = (Operator<Row>) myDatabase.getStorageInterface()
					.getTableByName(tName).getRows();
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		rowOp.open();

		Row r = null;

		int i = 0;

		for (i = 0; ((r = rowOp.next()) != null); i++) {

			String colNames[] = r.getColumnNames();

			assertEquals(r.getColumnCount(), 2);
			try {
				assertEquals(r.getColumnType("attribute1"), Types
						.getIntegerType());
				assertEquals(r.getColumnType("attribute2"), Types
						.getVarcharType());

			} catch (NoSuchColumnException e) {
				fail("unexpected");
			}

			/*
			 * no order on columns!!
			 */
			assertEquals(colNames.length, schema.size());
			assertTrue(colNames[0].equals("attribute1")
					|| colNames[0].equals("attribute2"));
			assertTrue(colNames[1].equals("attribute1")
					|| colNames[1].equals("attribute2"));
			assertTrue(!(colNames[0].equals(colNames[1])));

		}

	}

	/**
	 * 
	 */
	public void testDeleteRow() {

		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getIntegerType());
		tableSchema.put("attribute2", Types.getVarcharType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Row row1, row2, row3;

		ColumnInfo ci1 = new ColumnInfo();
		ColumnInfo ci2 = new ColumnInfo();

		ci1.setName("attribute1");
		ci1.setType(Types.getIntegerType());
		ci2.setName("attribute2");
		ci2.setType(Types.getVarcharType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci1);
		schema.add(ci2);
		Object[] arr1 = new Object[2];
		Object[] arr2 = new Object[2];
		Object[] arr3 = new Object[2];

		arr1[0] = new Integer(5);
		arr1[1] = "I am row 1";
		arr2[0] = new Integer(6);
		arr2[1] = "I am row 2";
		arr3[0] = new Integer(7);
		arr3[1] = "I am row 3";

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		int rowID = 0;
		try {
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row1);
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row2);
			rowID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row3);
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");
		}

		try {
			/* one time by id, one time by */
			myDatabase.getStorageInterface().getTableByName(tName).deleteRow(
					rowID);
			/*
			 * !! deletion based on contents of row, not the reference, I could
			 * have passed a different row object but with the same contents
			 */
			myDatabase.getStorageInterface().getTableByName(tName).deleteRow(
					row2);

		} catch (NoSuchRowException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		}

		// now let us see if we have only 1 row remaining, matching row 1

		Operator<Row> rowOp = null;

		try {
			rowOp = (Operator<Row>) myDatabase.getStorageInterface()
					.getTableByName(tName).getRows();
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		rowOp.open();
		int i = 0;
		Row r = null;
		for (i = 0; (r = rowOp.next()) != null; i++) {

			try {
				assertEquals(r.getColumnValue("attribute1"), new Integer(5));
				assertEquals(r.getColumnValue("attribute2"), "I am row 1");
			} catch (NoSuchColumnException e) {
				fail("unexpected");
			}

		}

		rowOp.close();

		assertTrue(i == 1);

	}

	/**
	 * 
	 */
	public void testUpdateRow() {

		Database myDatabase = Database.getInstance();

		// initialize table schema (empty schema)
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put("attribute1", Types.getIntegerType());
		tableSchema.put("attribute2", Types.getVarcharType());

		// start DB (should be empty before this stage)
		myDatabase.startSystem();

		// create 1 new table
		try {
			myDatabase.getStorageInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			fail("unexpected");
		}

		Row row1, row2, row3, row2_new, row3_new;

		ColumnInfo ci1 = new ColumnInfo();
		ColumnInfo ci2 = new ColumnInfo();

		ci1.setName("attribute1");
		ci1.setType(Types.getIntegerType());
		ci2.setName("attribute2");
		ci2.setType(Types.getVarcharType());
		List<ColumnInfo> schema = new ArrayList<ColumnInfo>();
		schema.add(ci1);
		schema.add(ci2);
		Object[] arr1 = new Object[2];
		Object[] arr2 = new Object[2];
		Object[] arr3 = new Object[2];
		Object[] arr2_new = new Object[2];
		Object[] arr3_new = new Object[2];

		arr1[0] = new Integer(5);
		arr1[1] = "I am row 1";
		arr2[0] = new Integer(6);
		arr2[1] = "I am row 2";
		arr3[0] = new Integer(7);
		arr3[1] = "I am row 3";

		arr2_new[0] = new Integer(6);
		arr2_new[1] = "I was row 2";
		arr3_new[0] = new Integer(7);
		arr3_new[1] = "I was row 3";

		row1 = new SampleRow(schema, arr1);
		row2 = new SampleRow(schema, arr2);
		row3 = new SampleRow(schema, arr3);

		row2_new = new SampleRow(schema, arr2_new);
		row3_new = new SampleRow(schema, arr3_new);

		int rowID = 0;
		try {
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row1);
			myDatabase.getStorageInterface().getTableByName(tName).addRow(row2);
			rowID = myDatabase.getStorageInterface().getTableByName(tName)
					.addRow(row3);
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");
		}

		try {
			/* one time by id, one time by */
			myDatabase.getStorageInterface().getTableByName(tName).updateRow(
					rowID, row3_new);
			/*
			 * !! deletion based on contents of row, not the reference, I could
			 * have passed a different row object but with the same contents
			 */
			myDatabase.getStorageInterface().getTableByName(tName).updateRow(
					row2, row2_new);

		} catch (NoSuchRowException e) {
			fail("Unexpected");
		} catch (NoSuchTableException e) {
			fail("Unexpected");
		} catch (SchemaMismatchException e) {
			fail("Unexpected");
		}

		// now let us see if we have only 1 row remaining, matching row 1

		Operator<Row> rowOp = null;

		try {
			rowOp = (Operator<Row>) myDatabase.getStorageInterface()
					.getTableByName(tName).getRows();
		} catch (NoSuchTableException e) {
			fail("unexpected");
		}

		rowOp.open();
		int i = 0;
		Row r = null;
		for (i = 0; (r = rowOp.next()) != null; i++) {
			try {
				if (r.getColumnValue("attribute1").equals(new Integer(5))) {
					assertEquals(r.getColumnValue("attribute2"), "I am row 1");
				} else if (r.getColumnValue("attribute1")
						.equals(new Integer(6))) {
					assertEquals(r.getColumnValue("attribute2"), "I was row 2");
				} else if (r.getColumnValue("attribute1")
						.equals(new Integer(7))) {
					assertEquals(r.getColumnValue("attribute2"), "I was row 3");
				} else {

					fail("no other rows should be retrieved!");
				}

			} catch (NoSuchColumnException e) {
				fail("unexpected");
			}

		}

		rowOp.close();

		assertTrue(i == 3);

	}
}
