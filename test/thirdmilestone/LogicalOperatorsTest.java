/**
 * 
 */
package thirdmilestone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import junit.framework.TestCase;
import metadata.Type;
import metadata.Types;
import operator.Operator;

import org.junit.After;
import org.junit.Before;

import relationalalgebra.CrossProduct;
import relationalalgebra.Input;
import relationalalgebra.Join;
import relationalalgebra.Projection;
import relationalalgebra.Selection;
import sampleDB.ColumnInfo;
import sampleDB.LeafPredicateTreeNode;
import sampleDB.SampleRow;
import systeminterface.Database;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.Table;
import util.ComparisonOperator;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * 
 * Test for Individual relational operators.
 * 
 * Example taken from Ramakrishnan & Gherke 3rd. Ed, Ch 4 (types differ)
 * 
 * S1, S2 instances of
 * Sailors(sid:INTEGER,sname:VARCHAR,rating:INTEGER,age:INTEGER) <br />
 * R1 instance of Reserves(sid:INTEGER, bid:INTEGER, day:VARCHAR)
 * 
 * <br />
 * Remember: no ordering on attributes or tuples assumed.
 * 
 * @author myahya
 * 
 * @version 273
 * 
 */
public class LogicalOperatorsTest extends TestCase {

	private Database myDatabase = Database.getInstance();

	// Table names
	String S1 = "S1";

	String S2 = "S2";

	String R1 = "R1";

	// Schemas
	HashMap<String, Type> S1_Schema = new HashMap<String, Type>();

	HashMap<String, Type> S2_Schema = new HashMap<String, Type>();

	HashMap<String, Type> R1_Schema = new HashMap<String, Type>();

	// Column Names
	// S1
	String sid1 = "sid1";

	String sname1 = "sname1";

	String rating1 = "rating1";

	String age1 = "age1";

	// S2
	String sid2 = "sid2";

	String sname2 = "sname2";

	String rating2 = "rating2";

	String age2 = "age2";

	// R1
	String sid = "sid";

	String bid = "bid";

	String day = "day";

	@Before
	public void setUp() {

		// table schemas

		S1_Schema.put(sid1, Types.getIntegerType());
		S1_Schema.put(sname1, Types.getVarcharType());
		S1_Schema.put(rating1, Types.getIntegerType());
		S1_Schema.put(age1, Types.getIntegerType());

		S2_Schema.put(sid2, Types.getIntegerType());
		S2_Schema.put(sname2, Types.getVarcharType());
		S2_Schema.put(rating2, Types.getIntegerType());
		S2_Schema.put(age2, Types.getIntegerType());

		R1_Schema.put(sid, Types.getIntegerType());
		R1_Schema.put(bid, Types.getIntegerType());
		R1_Schema.put(day, Types.getVarcharType());

		// table creation

		try {
			myDatabase.getQueryInterface().createTable(S1, S1_Schema);
			myDatabase.getQueryInterface().createTable(S2, S2_Schema);
			myDatabase.getQueryInterface().createTable(R1, R1_Schema);
		} catch (TableAlreadyExistsException e) {
			fail("TableAlreadyExistsException");
		}

		// Prepare rows

		List<ColumnInfo> schema_S1 = new ArrayList<ColumnInfo>();

		ColumnInfo S1_Col1 = new ColumnInfo(sid1, S1_Schema.get(sid1));
		ColumnInfo S1_Col2 = new ColumnInfo(sname1, S1_Schema.get(sname1));
		ColumnInfo S1_Col3 = new ColumnInfo(rating1, S1_Schema.get(rating1));
		ColumnInfo S1_Col4 = new ColumnInfo(age1, S1_Schema.get(age1));

		schema_S1.add(S1_Col1);
		schema_S1.add(S1_Col2);
		schema_S1.add(S1_Col3);
		schema_S1.add(S1_Col4);

		List<ColumnInfo> schema_S2 = new ArrayList<ColumnInfo>();

		ColumnInfo S2_Col1 = new ColumnInfo(sid2, S2_Schema.get(sid2));
		ColumnInfo S2_Col2 = new ColumnInfo(sname2, S2_Schema.get(sname2));
		ColumnInfo S2_Col3 = new ColumnInfo(rating2, S2_Schema.get(rating2));
		ColumnInfo S2_Col4 = new ColumnInfo(age2, S2_Schema.get(age2));

		schema_S2.add(S2_Col1);
		schema_S2.add(S2_Col2);
		schema_S2.add(S2_Col3);
		schema_S2.add(S2_Col4);

		List<ColumnInfo> schema_R1 = new ArrayList<ColumnInfo>();

		ColumnInfo R1_Col1 = new ColumnInfo(sid, R1_Schema.get(sid));
		ColumnInfo R1_Col2 = new ColumnInfo(bid, R1_Schema.get(bid));
		ColumnInfo R1_Col3 = new ColumnInfo(day, R1_Schema.get(day));

		schema_R1.add(R1_Col1);
		schema_R1.add(R1_Col2);
		schema_R1.add(R1_Col3);

		// insert rows
		try {
			myDatabase.getQueryInterface().insertRow(
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 22,
							"Dustin", 7, 45 }))));
			myDatabase.getQueryInterface().insertRow(
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 31,
							"Lubber", 8, 55 }))));
			myDatabase.getQueryInterface().insertRow(
					S1,
					(Row) ((new SampleRow(schema_S1, new Object[] { 58,
							"Rusty", 10, 35 }))));

			myDatabase.getQueryInterface().insertRow(
					S2,
					(Row) ((new SampleRow(schema_S2, new Object[] { 28,
							"yuppy", 9, 35 }))));
			myDatabase.getQueryInterface().insertRow(
					S2,
					(Row) ((new SampleRow(schema_S2, new Object[] { 31,
							"Lubber", 8, 55 }))));
			myDatabase.getQueryInterface().insertRow(
					S2,
					(Row) ((new SampleRow(schema_S2, new Object[] { 44,
							"guppy", 5, 35 }))));
			myDatabase.getQueryInterface().insertRow(
					S2,
					(Row) ((new SampleRow(schema_S2, new Object[] { 58,
							"Rusty", 10, 35 }))));

			myDatabase.getQueryInterface().insertRow(
					R1,
					(Row) ((new SampleRow(schema_R1, new Object[] { 22, 101,
							"101096" }))));
			myDatabase.getQueryInterface().insertRow(
					R1,
					(Row) ((new SampleRow(schema_R1, new Object[] { 58, 103,
							"111296" }))));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		}

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {

		// delete all tables
		Operator<? extends Table> op = myDatabase.getStorageInterface()
				.getTables();

		ArrayList<String> tableNames = new ArrayList<String>();
		Table t;
		op.open();
		while ((t = op.next()) != null) {

			tableNames.add(t.getTableName());
		}
		op.close();

		Iterator<String> it = tableNames.iterator();
		while (it.hasNext()) {

			myDatabase.getStorageInterface().deleteTable(it.next());
		}

	}

	/**
	 * Test Input
	 */
	public void testInput() {

		System.out.println("testInput");

		String[] expectedSchema = { sid1, sname1, rating1, age1 };
		int expectedResultSize = 3;
		TreeSet<String> expectedSName1ResultSet = new TreeSet<String>();
		expectedSName1ResultSet.add("Dustin");
		expectedSName1ResultSet.add("Lubber");
		expectedSName1ResultSet.add("Rusty");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getQueryInterface().query(new Input(S1));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		}

		TreeSet<String> SName1ResultSet = new TreeSet<String>();

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null)

		{

			count++;

			assertTrue(checkSchema(r.getColumnNames(), expectedSchema));

			try {
				SName1ResultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();
		}

		rowOp.close();

		assertEquals(expectedResultSize, count);
		assertTrue(SName1ResultSet.equals(expectedSName1ResultSet));

	}

	/**
	 * Test Selection
	 */
	public void testSelection() {

		System.out.println("testSelection");

		String[] expectedSchema = { sid2, sname2, rating2, age2 };
		int expectedResultSize = 2;
		TreeSet<String> expectedSName2ResultSet = new TreeSet<String>();
		expectedSName2ResultSet.add("yuppy");
		expectedSName2ResultSet.add("Rusty");

		// selection predicate
		PredicateTreeNode predicate = new LeafPredicateTreeNode(rating2,
				ComparisonOperator.GT, new Integer(8));

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getQueryInterface().query(
					new Selection(new Input(S2), predicate));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		}

		TreeSet<String> SName2ResultSet = new TreeSet<String>();

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			assertTrue(checkSchema(r.getColumnNames(), expectedSchema));

			try {
				SName2ResultSet.add((String) r.getColumnValue(sname2));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();

		}

		rowOp.close();

		assertEquals(expectedResultSize, count);
		assertTrue(SName2ResultSet.equals(expectedSName2ResultSet));

	}

	/**
	 * Test Projection
	 */
	public void testProjection() {

		System.out.println("testProjection");

		String[] expectedSchema = { sname2, rating2 };
		int expectedResultSize = 4;
		TreeSet<String> expectedSName2ResultSet = new TreeSet<String>();
		expectedSName2ResultSet.add("yuppy");
		expectedSName2ResultSet.add("Rusty");
		expectedSName2ResultSet.add("Lubber");
		expectedSName2ResultSet.add("guppy");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getQueryInterface().query(
					new Projection(new Input(S2), new String[] { sname2,
							rating2 }));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		}

		TreeSet<String> SName2ResultSet = new TreeSet<String>();

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			assertTrue(checkSchema(r.getColumnNames(), expectedSchema));

			try {
				SName2ResultSet.add((String) r.getColumnValue(sname2));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();

		}

		rowOp.close();

		assertEquals(expectedResultSize, count);
		assertTrue(SName2ResultSet.equals(expectedSName2ResultSet));

	}

	/**
	 * Test Joins
	 */
	public void testJoin() {

		System.out.println("testJoin");

		String[] expectedSchema = { sid1, sname1, rating1, age1, sid, bid, day };
		int expectedResultSize = 2;
		TreeSet<String> expectedSName1ResultSet = new TreeSet<String>();
		expectedSName1ResultSet.add("Dustin");
		expectedSName1ResultSet.add("Rusty");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getQueryInterface().query(
					new Join(new Input(S1), new Input(R1), sid1, sid));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		}

		TreeSet<String> SName1ResultSet = new TreeSet<String>();

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			assertTrue(checkSchema(r.getColumnNames(), expectedSchema));

			try {
				SName1ResultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();

		}

		rowOp.close();

		assertEquals(expectedResultSize, count);
		assertTrue(SName1ResultSet.equals(expectedSName1ResultSet));

	}

	/**
	 * Test Cross Product
	 */
	public void testCrossProduct() {

		System.out.println("testCrossProduct");

		String[] expectedSchema = { sid1, sname1, rating1, age1, sid, bid, day };
		int expectedResultSize = 6;
		TreeSet<String> expectedSName1ResultSet = new TreeSet<String>();
		expectedSName1ResultSet.add("Dustin");
		expectedSName1ResultSet.add("Rusty");
		expectedSName1ResultSet.add("Lubber");

		Operator<? extends Row> rowOp = null;

		try {
			rowOp = myDatabase.getQueryInterface().query(
					new CrossProduct(new Input(S1), new Input(R1)));
		} catch (NoSuchTableException e) {
			fail("NoSuchTableException");
		} catch (SchemaMismatchException e) {
			fail("SchemaMismatchException");
		} catch (NoSuchColumnException e) {
			fail("NoSuchColumnException");
		} catch (InvalidPredicateException e) {
			fail("InvalidPredicateException");
		}

		TreeSet<String> SName1ResultSet = new TreeSet<String>();

		rowOp.open();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			assertTrue(checkSchema(r.getColumnNames(), expectedSchema));

			try {
				SName1ResultSet.add((String) r.getColumnValue(sname1));
			} catch (NoSuchColumnException e) {
				fail("NoSuchColumnException");
			}

			r = rowOp.next();

		}

		rowOp.close();

		assertEquals(expectedResultSize, count);
		assertTrue(SName1ResultSet.equals(expectedSName1ResultSet));

	}

	private boolean checkSchema(String[] returnedSchema, String[] expectedSchema) {

		if (returnedSchema.length != expectedSchema.length) {
			return false;
		}

		int len = returnedSchema.length;

		// no order on attributes -> deal with sets

		TreeSet<String> returnedAttributeSet, expectedAttributeSet;
		returnedAttributeSet = new TreeSet<String>();
		expectedAttributeSet = new TreeSet<String>();

		for (int i = 0; i < len; i++) {
			returnedAttributeSet.add(returnedSchema[i]);
			expectedAttributeSet.add(expectedSchema[i]);
		}

		return expectedAttributeSet.equals(returnedAttributeSet);

	}

	/*
	 * Your tests go here
	 */

}
