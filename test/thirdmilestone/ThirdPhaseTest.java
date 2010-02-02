/**
 * 
 */
package thirdmilestone;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import metadata.Type;
import metadata.Types;
import operator.Operator;
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
import util.ComparisonOperator;
import util.Helpers;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;

/**
 * 
 * Data used in this test is from TPC Benchmark H (TPC-H).
 * http://www.tpc.org/tpch/
 * 
 * Please do not pre-compute results, the queries we will run will be varied ;)
 * 
 * Each query has some room for optimization.
 * 
 * 
 * @author myahya
 * 
 */
public class ThirdPhaseTest {

	private static Database myDatabase = Database.getInstance();

	/**
	 * Data dir
	 */
	private static final String dataDir = "data/";

	/**
	 * 1 entry per line tableName tableDataFile Att1 Att1Type Att2 Att2Type Att3
	 * CHAR CHAR_SIZE.
	 */
	private static final String tableInfoFile = "tableinfo.txt";

	/**
	 * table_name to table_schema
	 */
	private final static HashMap<String, Map<String, Type>> tableNameToSchema = new HashMap<String, Map<String, Type>>();

	/**
	 * table_name to ordered_schema
	 */
	private static final HashMap<String, ArrayList<ColumnInfo>> tableNameToOrdredSchema = new HashMap<String, ArrayList<ColumnInfo>>();

	/**
	 * A regular expression, used with String.split(String regex)
	 */
	private static final String attributeDelimiter = "\\|";

	/**
	 * Initialize database from file
	 */
	private static void loadTableData() {

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(dataDir + tableInfoFile));

			while (true) {

				// read next line
				String line = in.readLine();
				if (line == null) {
					break;
				}

				String[] tokens = line.split(" ");
				String tableName = tokens[0];
				String tableFileName = tokens[1];

				tableNameToSchema.put(tableName, new HashMap<String, Type>());
				tableNameToOrdredSchema.put(tableName,
						new ArrayList<ColumnInfo>());

				// attributes data
				for (int i = 2; i < tokens.length;) {

					String attName = tokens[i++];
					String attTypeName = (tokens[i++]);

					Type attType = null;

					/*
					 * Undefined, how to represent dates, crazy longs probably
					 * won't need this for now...
					 */
					if (attTypeName.equals("CHAR")) {
						attType = Types.getCharType(Integer
								.valueOf(tokens[i++]));
					} else if (attTypeName.equals("DATE")) {
						attType = Types.getDateType();
					} else if (attTypeName.equals("DOUBLE")) {
						attType = Types.getDoubleType();
					} else if (attTypeName.equals("FLOAT")) {
						attType = Types.getFloatType();
					} else if (attTypeName.equals("INTEGER")) {
						attType = Types.getIntegerType();
					} else if (attTypeName.equals("LONG")) {
						attType = Types.getLongType();
					} else if (attTypeName.equals("VARCHAR")) {
						attType = Types.getVarcharType();
					} else {
						throw new RuntimeException("Invalid type: "
								+ attTypeName);
					}

					tableNameToSchema.get(tableName).put(attName, attType);

					ColumnInfo ci = new ColumnInfo(attName, attType);
					tableNameToOrdredSchema.get(tableName).add(ci);

				}

				// at this point, table info loaded.

				// Create table
				myDatabase.getQueryInterface().createTable(tableName,
						tableNameToSchema.get(tableName));

				// Now, load data into newly created table
				readTable(tableName, tableFileName);

			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		}

	}

	/**
	 * 
	 * Load table data from file and insert it into DB.
	 * 
	 * @param tableName
	 *            Table name
	 * @param filename
	 *            File containing table data
	 */
	public static void readTable(String tableName, String filename) {

		// System.out.println(filename);

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(dataDir + filename));

			while (true) {

				String line = in.readLine();

				if (line == null) {
					break;
				}
				// remove | at end for tokenization
				line = line.substring(0, line.length() - 1);

				// line into tokens
				String[] tokens = line.split(attributeDelimiter);
				int numAttributes = tokens.length;

				assert (tokens.length == tableNameToOrdredSchema.get(tableName)
						.size());

				Object[] arr = new Object[numAttributes];

				for (int i = 0; i < tokens.length; i++) {

					Class<?> type = tableNameToOrdredSchema.get(tableName).get(
							i).getType().getCorrespondingJavaClass();
					Object val = null;

					if (type.isAssignableFrom(String.class)) {
						val = tokens[i];
					} else if (type.isAssignableFrom(java.util.Date.class)) {
						val = new java.util.Date(tokens[i]);
					} else if (type.isAssignableFrom(java.lang.Integer.class)) {
						val = Integer.parseInt(tokens[i]);
					} else if (type.isAssignableFrom(java.lang.Double.class)) {
						val = Double.parseDouble(tokens[i]);
					} else if (type.isAssignableFrom(java.lang.Long.class)) {
						val = Long.parseLong(tokens[i]);
					} else if (type.isAssignableFrom(java.lang.Float.class)) {
						val = Float.parseFloat(tokens[i]);
					} else {
						throw new RuntimeException("Type problem");
					}

					arr[i] = val;

				}

				SampleRow row = new SampleRow(tableNameToOrdredSchema
						.get(tableName), arr);

				myDatabase.getQueryInterface().insertRow(tableName, row);

			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Query 1
	 * 
	 * @return List with results
	 */
	public static List<String> performQuery1() {

		/*
		 * Query: Names of all suppliers in the middle east.
		 * 
		 * 
		 * Query plan given here:
		 * 
		 * 1. Join Suppliers with Nation, join condition s_nationkey =
		 * n_nationkey
		 * 
		 * 2. Join result of (1) with Region, join condition n_regionkey =
		 * r_regionkey
		 * 
		 * 3. Select from result of (2) tuples with r_name = "MIDDLE EAST"
		 * 
		 * 4. Project result of (3) on s_name
		 */

		// Inputs
		Input S = new Input("Supplier");
		Input N = new Input("Nation");
		Input R = new Input("Region");

		// Joins
		Join S_join_N = new Join(S, N, "s_nationkey", "n_nationkey");
		Join S_join_N_join_R = new Join(S_join_N, R, "n_regionkey",
				"r_regionkey");

		// Select
		PredicateTreeNode predicate = new LeafPredicateTreeNode("r_name",
				ComparisonOperator.EQ, new String("MIDDLE EAST"));
		Selection sel = new Selection(S_join_N_join_R, predicate);

		// Project
		Projection proj = new Projection(sel, new String[] { "s_name" });

		Operator<? extends Row> rowOp = null;

		// Execute Query
		try {
			rowOp = myDatabase.getQueryInterface().query(proj);
		} catch (NoSuchTableException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (SchemaMismatchException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (NoSuchColumnException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (InvalidPredicateException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		}

		rowOp.open();

		ArrayList<String> results = new ArrayList<String>();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			try {
				results.add((String) r.getColumnValue("s_name"));
			} catch (NoSuchColumnException e) {
				Helpers.print(e.getStackTrace().toString(),
						util.Consts.printType.ERROR);
			}

			r = rowOp.next();

		}

		rowOp.close();

		return results;

	}

	/**
	 * Query 2
	 * 
	 * @return List with results
	 */
	public static List<Double> performQuery2() {

		/*
		 * What is the account balance suppliers with an account balance of more
		 * than 1000.00
		 */

		// create an index on s_acctbal
		try {
			myDatabase.getIndexInterface().createIndex("s_acctbal_idx",
					"Supplier", "s_acctbal", true);
		} catch (IndexAlreadyExistsException e1) {
			Helpers.print(e1.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (SchemaMismatchException e1) {
			Helpers.print(e1.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (NoSuchTableException e1) {
			Helpers.print(e1.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		}

		// Inputs
		Input S = new Input("Supplier");

		// Select
		PredicateTreeNode predicate = new LeafPredicateTreeNode("s_acctbal",
				ComparisonOperator.GT, new Double(1000.00));
		Selection sel = new Selection(S, predicate);

		// Project
		Projection proj = new Projection(sel, new String[] { "s_acctbal" });

		Operator<? extends Row> rowOp = null;

		// Execute Query
		try {
			rowOp = myDatabase.getQueryInterface().query(proj);
		} catch (NoSuchTableException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (SchemaMismatchException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (NoSuchColumnException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (InvalidPredicateException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		}

		rowOp.open();

		List<Double> results = new ArrayList<Double>();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			try {
				results.add((Double) r.getColumnValue("s_acctbal"));
			} catch (NoSuchColumnException e) {
				Helpers.print(e.getStackTrace().toString(),
						util.Consts.printType.ERROR);
			}

			r = rowOp.next();

		}

		rowOp.close();

		return results;

	}

	/**
	 * Query 3
	 * 
	 * @return List with results
	 */
	public static List<String> performQuery3() {

		/*
		 * Query: Names of all suppliers in the middle east.
		 * 
		 * 
		 * Query plan given here:
		 * 
		 * 1. Join Suppliers with Nation, join condition s_nationkey =
		 * n_nationkey
		 * 
		 * 2. Join result of (1) with Region, join condition n_regionkey =
		 * r_regionkey
		 * 
		 * 3. Select from result of (2) tuples with r_name = "MIDDLE EAST"
		 * 
		 * 4. Project result of (3) on s_name
		 */

		// Inputs
		Input S = new Input("Supplier");
		Input N = new Input("Nation");
		Input R = new Input("Region");

		// Joins
		Join S_join_N = new Join(S, N, "s_nationkey", "n_nationkey");
		Join S_join_N_join_R = new Join(S_join_N, R, "n_regionkey",
				"r_regionkey");

		// Select
		PredicateTreeNode predicate = new LeafPredicateTreeNode("r_name",
				ComparisonOperator.EQ, new String("AFRICA"));
		Selection sel = new Selection(S_join_N_join_R, predicate);

		// Project
		Projection proj = new Projection(sel, new String[] { "s_name" });

		Operator<? extends Row> rowOp = null;

		// Execute Query
		try {
			rowOp = myDatabase.getQueryInterface().query(proj);
		} catch (NoSuchTableException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (SchemaMismatchException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (NoSuchColumnException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		} catch (InvalidPredicateException e) {
			Helpers.print(e.getStackTrace().toString(),
					util.Consts.printType.ERROR);
		}

		rowOp.open();

		ArrayList<String> results = new ArrayList<String>();

		Row r = rowOp.next();
		int count = 0;

		while (r != null) {

			count++;

			try {
				results.add((String) r.getColumnValue("s_name"));
			} catch (NoSuchColumnException e) {
				Helpers.print(e.getStackTrace().toString(),
						util.Consts.printType.ERROR);
			}

			r = rowOp.next();

		}

		rowOp.close();

		return results;

	}

	/**
	 * Test steps
	 */
	public static void test() {

		long time = 0;
		long timeBegin, timeEnd;

		// Start DB
		myDatabase.startSystem();

		// load tables
		loadTableData();

		timeBegin = System.currentTimeMillis();
		// Query1
		List<String> q1Result = performQuery1();
		timeEnd = System.currentTimeMillis();


		time += (timeEnd - timeBegin);

		// Check Query results
		if (!checkStringQueryResults(q1Result, "select_me_suppliers.rslt")) {
			Helpers.print("Query 1 results did not match what is expected",
					util.Consts.printType.ERROR);
		}

		timeBegin = System.currentTimeMillis();
		// Query2
		List<Double> q2Result = performQuery2();
		timeEnd = System.currentTimeMillis();


		
		time += (timeEnd - timeBegin);

		if (!checkDoubleQueryResults(q2Result,
				"select_supplier_acctbal_over_1000.rslt")) {
			Helpers.print("Query 2 results did not match what is expected",
					util.Consts.printType.ERROR);
		}

		timeBegin = System.currentTimeMillis();
		// Query 3
		List<String> q3Result = performQuery3();


		timeEnd = System.currentTimeMillis();

		time += (timeEnd - timeBegin);

		// Check Query results
		if (!checkStringQueryResults(q3Result, "select_af_suppliers.rslt")) {
			Helpers.print("Query 3 results did not match what is expected",
					util.Consts.printType.ERROR);
		}

		System.out.println("Time= " + time);

		// myDatabase.shutdownSystem();

	}

	private static boolean checkStringQueryResults(List<String> results,
			String resultsFileName) {

		List<String> expectedResults = new ArrayList<String>();

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(dataDir + resultsFileName));

			while (true) {

				String line = in.readLine();

				if (line == null) {
					break;
				}

				// works for us, might not be the general case
				line = line.trim();

				expectedResults.add(new String(line));
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(results);
		Collections.sort(expectedResults);

		if (!expectedResults.equals(results))
			return false;
		else
			return true;
	}

	private static boolean checkDoubleQueryResults(List<Double> results,
			String resultsFileName) {

		List<Double> expectedResults = new ArrayList<Double>();

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(dataDir + resultsFileName));

			while (true) {

				String line = in.readLine();

				if (line == null) {
					break;
				}

				line = line.trim();

				expectedResults.add(new Double(Double.parseDouble(line)));
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(results);
		Collections.sort(expectedResults);

		if (!expectedResults.equals(results))
			return false;
		else
			return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		System.out.println("*************************************");
		System.out.println("Testing Started");
		test();
		System.out.println("Testing Finished");
		System.out.println("*************************************");

		/*
		 * try { System.out.println((SampleTable)
		 * myDatabase.getStorageInterface() .getTableByName("Customer"));
		 * System.out.println(); System.out.println((SampleTable)
		 * myDatabase.getStorageInterface() .getTableByName("Nation"));
		 * System.out.println(); System.out.println((SampleTable)
		 * myDatabase.getStorageInterface() .getTableByName("Region"));
		 * System.out.println(); System.out.println((SampleTable)
		 * myDatabase.getStorageInterface() .getTableByName("Supplier")); }
		 * catch (NoSuchTableException e) { e.printStackTrace(); }
		 */
	}

}
