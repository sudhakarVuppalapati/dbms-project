package firstmilestone;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import metadata.Type;
import metadata.Types;
import operator.Operator;
import sampleDB.ColumnInfo;
import sampleDB.SampleRow;
import systeminterface.Column;
import systeminterface.Database;
import systeminterface.Row;
import systeminterface.Table;
import util.Consts;
import util.Helpers;
import util.RandomInputGenerator;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * 
 * This test is focused on correctness. It is not meant to represent any
 * specific workload (which is something that we have yet to define). You can
 * play with the parameters of the test in util.Consts.
 * 
 */
public class FirstPhaseTest {

	private static class TestTable {

		public int initialCardinaliy;

		public int cardinality;

		/* schema for table creation */
		public Map<String, Type> mapSchema;

		public final ArrayList<Integer> rowIDs = new ArrayList<Integer>();

		public final ArrayList<ColumnInfo> schema = new ArrayList<ColumnInfo>();

		/*
		 * rowIDs in same order as rows
		 */
		// actual table data
		public final ArrayList<ArrayList<Object>> tableContents = new ArrayList<ArrayList<Object>>();

		public String tableName;

		// some more random data with same schema. source for new rows when
		// adding/updating
		public final ArrayList<ArrayList<Object>> updateContents = new ArrayList<ArrayList<Object>>();
	}

	/* number of tables (can change when tables deleted */
	private static int currentNumberOfTables = util.Consts.numTables;

	/* DB */
	private static final Database myDatabase = Database.getInstance();

	/* Random number generator -- add seed when repeatable experiments wanted */
	private static final Random rand = new Random(Consts.seed);

	/* own local rep of tables */
	private static final ArrayList<TestTable> tableList = new ArrayList<TestTable>();

	private static boolean checkCorrectness() {

		/** *************************************************** */
		/*
		 * Does DB contain same tables I have?
		 */
		if (!checkTables()) {
			return false;
		}

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Check Table Schemas
		 */
		if (!checkTableSchemas()) {
			return false;
		}

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Check table contents & cardinalities
		 */
		if (!checkTableContents()) {
			return false;
		}

		/** *************************************************** */

		return true;

	}

	/**
	 * Check if table contents match what we expect
	 * 
	 * @return true/false
	 */
	private static boolean checkTableContents() {

		int i = 0;
		for (i = 0; i < currentNumberOfTables; i++) {

			Operator<Row> rowOp = null;

			try {
				rowOp = (Operator<Row>) myDatabase.getStorageInterface()
						.getTableByName(tableList.get(i).tableName).getRows();

			} catch (NoSuchTableException e) {
				e.printStackTrace();
			}

			rowOp.open();

			Row r = null;
			int j = 0;
			for (j = 0; (r = rowOp.next()) != null; j++) {

				/* Let us see if row is in our local representation on the table */

				// the supplied row is transformed to a list with same order as
				// schema
				ArrayList<Object> inputRowAsList = new ArrayList<Object>(
						tableList.get(i).schema.size());

				for (int k = 0; k < tableList.get(i).schema.size(); k++) {

					try {

						inputRowAsList.add(k, r
								.getColumnValue(tableList.get(i).schema.get(k)
										.getName()));
					} catch (NoSuchColumnException e) {
						e.printStackTrace();
					}
				}

				// now supplied row in a list, let us do a table scan.
				boolean rowMatched = false;
				for (int k = 0; k < tableList.get(i).cardinality; k++) {

					int l = 0;
					for (l = 0; l < tableList.get(i).schema.size(); l++) {

						// k: row #
						// l: column #
						// i: table #
						/* go over all attributes of row # k */
						if (!(tableList.get(i).tableContents.get(l).get(k)
								.equals(inputRowAsList.get(l)))) {

							break;
						}

					}

					// Row matched all columns successfully
					if ((l == tableList.get(i).schema.size())) {
						rowMatched = true;
						break;

					}

				}

				if (!rowMatched) {
					Helpers.print("Row from table could not be matched",
							util.Consts.printType.ERROR);
					return false;

				}

			}
			rowOp.close();

			// check for matching cardinalities
			if (j != tableList.get(i).cardinality) {
				Helpers.print("Cardinality mismatch",
						util.Consts.printType.ERROR);
				return false;

			}

			Helpers.print("Cardinality=" + j, util.Consts.printType.INFO);

		}
		return true;
	}

	private static boolean checkTables() {
		Operator<Table> tableOp = (Operator<Table>) myDatabase
				.getStorageInterface().getTables();

		tableOp.open();

		Table t = null;
		int i = 0;
		HashSet<String> retrievedTableNames = new HashSet<String>();
		for (i = 0; (t = tableOp.next()) != null; i++) {

			retrievedTableNames.add(t.getTableName());

		}
		tableOp.close();

		if (retrievedTableNames.size() != currentNumberOfTables) {
			Helpers.print("Incorrect num of tables retrieved",
					util.Consts.printType.ERROR);
			return false;
		} else {

			for (int j = 0; j < currentNumberOfTables; j++) {

				if (!retrievedTableNames.contains(tableList.get(j).tableName)) {
					Helpers.print("Expected table not found",
							util.Consts.printType.ERROR);
					return false;

				}
			}
		}

		return true;

	}

	private static boolean checkTableSchemas() {

		int i = 0;
		for (i = 0; i < currentNumberOfTables; i++) {

			Operator<Column> colOp = null;

			try {
				colOp = (Operator<Column>) myDatabase.getStorageInterface()
						.getTableByName(tableList.get(i).tableName)
						.getAllColumns();

			} catch (NoSuchTableException e) {
				e.printStackTrace();
			}

			colOp.open();

			Column col = null;
			int j = 0;
			for (j = 0; (col = colOp.next()) != null; j++) {

				if (!tableList.get(i).mapSchema
						.containsKey(col.getColumnName())
						|| !col.getColumnType().toString().equals(
								tableList.get(i).mapSchema.get(
										col.getColumnName()).toString())) {

					Helpers.print("Schema mismatch found",
							util.Consts.printType.ERROR);
					return false;

				}
			}
			colOp.close();

			if (j != tableList.get(i).mapSchema.size()) {
				Helpers.print("Schema size mismatch",
						util.Consts.printType.ERROR);
				return false;

			}
		}
		return true;
	}

	/**
	 * generate tables in DB
	 */
	private static void createTablesInDB() {
		for (int i = 0; i < currentNumberOfTables; i++) {

			try {
				myDatabase.getStorageInterface().createTable(
						tableList.get(i).tableName, tableList.get(i).mapSchema);
			} catch (TableAlreadyExistsException e) {
				e.printStackTrace();
			}

		}
	}

	private static void deleteRowInLocal(int tableNumber, Object[] rowArray) {
		if (rowArray.length != tableList.get(tableNumber).schema.size()) {

			throw new RuntimeException();

		}

		for (int i = 0; i < tableList.get(tableNumber).cardinality; i++) {

			boolean mismatchFound = false;

			for (int j = 0; j < tableList.get(tableNumber).schema.size(); j++) {

				// i:row
				// j:column
				if (!tableList.get(tableNumber).tableContents.get(j).get(i)
						.equals(rowArray[j])) {
					mismatchFound = true;
				}
			}

			if (!mismatchFound) {
				removeRowFromTable(tableNumber, i);
				i--;
			}
		}
	}

	/**
	 * Load initial data into tables
	 */
	private static void fillDBTables() {

		for (int i = 0; i < currentNumberOfTables; i++) {

			String tableName = tableList.get(i).tableName;
			int tableCardinality = tableList.get(i).cardinality;
			int tableDimension = tableList.get(i).schema.size();

			// go over all rows in a table
			for (int j = 0; j < tableCardinality; j++) {
				// generate new row
				Object objArr[] = new Object[tableDimension];
				for (int k = 0; k < tableDimension; k++) {
					// i: table num
					// k: attribute order
					// j: row number
					objArr[k] = tableList.get(i).tableContents.get(k).get(j);
				}
				SampleRow row = new SampleRow(tableList.get(i).schema, objArr);

				int rowID = 0;

				try {
					rowID = myDatabase.getStorageInterface().getTableByName(
							tableName).addRow(row);
				} catch (SchemaMismatchException e) {
					e.printStackTrace();
				} catch (NoSuchTableException e) {
					e.printStackTrace();
				}

				// keep track of returned row IDs (all in order)
				tableList.get(i).rowIDs.add(rowID);

			}

			// do table printing
			/*
			 * try {
			 * 
			 * Helpers.print(myDatabase.getStorageInterface().getTableByName(
			 * tableName).toString(), util.Consts.printType.INFO); } catch
			 * (NoSuchTableException e) { e.printStackTrace(); }
			 */

		}

	}

	/**
	 * generate tables with rand data
	 */
	private static void generateRandomTables() {

		for (int i = 0; i < currentNumberOfTables; i++) {

			int tableCardinatlity = rand.nextInt(util.Consts.maxCardinality
					- util.Consts.minCardinality + 1)
					+ util.Consts.minCardinality;

			Helpers.print("Table " + i + " Cardinality = " + tableCardinatlity,
					util.Consts.printType.INFO);

			tableList.get(i).cardinality = tableCardinatlity;
			tableList.get(i).initialCardinaliy = tableCardinatlity;
			// fill in each column with rand values

			for (int j = 0; j < tableList.get(i).schema.size(); j++) {

				ArrayList<Object> column = null;
				ArrayList<Object> backupColumn = null;

				if (tableList.get(i).schema.get(j).getType().equals(
						Types.getDateType())) {

					Helpers.print("Rand Date List", util.Consts.printType.INFO);

					column = RandomInputGenerator.getRandDateList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandDateList(rand,
							tableCardinatlity);

				}

				else if (tableList.get(i).schema.get(j).getType().equals(
						Types.getDoubleType())) {

					Helpers.print("Rand Double List",
							util.Consts.printType.INFO);
					column = RandomInputGenerator.getRandDoubleList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandDoubleList(rand,
							tableCardinatlity);

				}

				else if (tableList.get(i).schema.get(j).getType().equals(
						Types.getFloatType())) {

					Helpers
							.print("Rand Float List",
									util.Consts.printType.INFO);
					column = RandomInputGenerator.getRandFloatList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandFloatList(rand,
							tableCardinatlity);

				} 

				else if (tableList.get(i).schema.get(j).getType().equals(
						Types.getIntegerType())) {

					Helpers.print("Rand Integer List",
							util.Consts.printType.INFO);
					column = RandomInputGenerator.getRandIntegerList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandIntegerList(
							rand, tableCardinatlity);

				}

				else if (tableList.get(i).schema.get(j).getType().equals(
						Types.getLongType())) {

					Helpers.print("Rand Long List", util.Consts.printType.INFO);
					column = RandomInputGenerator.getRandLongList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandLongList(rand,
							tableCardinatlity);

				}

				else if (tableList.get(i).schema.get(j).getType().equals(
						Types.getVarcharType())) {

					Helpers.print("Rand VARCHAR List",
							util.Consts.printType.INFO);
					column = RandomInputGenerator.getRandVarcharList(rand,
							tableCardinatlity);
					backupColumn = RandomInputGenerator.getRandVarcharList(
							rand, tableCardinatlity);

				}

				else if (tableList.get(i).schema.get(j).getType().getLength() != -1) {

					Helpers.print("Rand CHAR List, max len = "
							+ tableList.get(i).schema.get(j).getType()
									.getLength(), util.Consts.printType.INFO);

					column = RandomInputGenerator.getRandCharList(rand,
							tableCardinatlity, tableList.get(i).schema.get(j)
									.getType().getLength());
					backupColumn = RandomInputGenerator.getRandCharList(rand,
							tableCardinatlity, tableList.get(i).schema.get(j)
									.getType().getLength());

				}

				tableList.get(i).tableContents.add(column);

				tableList.get(i).updateContents.add(backupColumn);
			}

		}

	}

	private static void initializeLocalTables() {

		for (int i = 0; i < currentNumberOfTables; i++) {

			TestTable t = new TestTable();

			// 1. Generate random Schema(s)
			t.mapSchema = RandomInputGenerator.generateRandSchema(rand);

			// turn schema into list
			Iterator<String> it = t.mapSchema.keySet().iterator();

			while (it.hasNext()) {
				String key = it.next();
				ColumnInfo ci = new ColumnInfo();
				ci.setName(key);
				ci.setType(t.mapSchema.get(key));
				t.schema.add(ci);
				// column for attribute
				// Done later, see rand data generation
				// t.tableContents.add(new ArrayList<Object>());
			}

			t.tableName = "Table" + i;

			tableList.add(t);
		}

	}

	/**
	 * @param args
	 *            args
	 */
	public static void main(String[] args) {
		FirstPhaseTest.test();

	}

	private static void manipulateContents(Random rand) {

		for (int i = 0; i < currentNumberOfTables; i++) {

			for (int k = 0; k < tableList.get(i).schema.size(); k++) {
				if (tableList.get(i).cardinality != tableList.get(i).tableContents
						.get(k).size()) {

					System.out.println("MISATCH");
					System.exit(-1);
				}
			}

			if (tableList.get(i).cardinality < 1) {

				continue;
			}

			for (int j = 0; j < tableList.get(i).cardinality
					* util.Consts.percentManipulate; j++) {

				int operation = rand.nextInt(11);

				int offsetInLocal = rand.nextInt(tableList.get(i).cardinality);
				int offsetInUpdateContents = rand
						.nextInt(tableList.get(i).initialCardinaliy);

				/*
				 * Update by row ID
				 */
				if (operation <= util.Consts.probUpdateByRowID) {

					int rowID = tableList.get(i).rowIDs.get(offsetInLocal);
					Object newRowArray[] = new Object[tableList.get(i).schema
							.size()];

					for (int k = 0; k < tableList.get(i).schema.size(); k++) {

						// k: column number
						// offsetInLocal: row number, locally
						newRowArray[k] = tableList.get(i).updateContents.get(k)
								.get(offsetInUpdateContents);
					}

					SampleRow newRow = new SampleRow(tableList.get(i).schema,
							newRowArray);

					try {
						myDatabase.getStorageInterface().getTableByName(
								tableList.get(i).tableName).updateRow(rowID,
								newRow);
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}

					updateRowInLocal(i, offsetInLocal, newRowArray);

					Helpers.print("Updated a row by row ID",
							util.Consts.printType.INFO);

				} else if (operation <= util.Consts.probUpdateByRowValues) {

					/*
					 * Update a row by values
					 */

					Object oldRowArray[] = new Object[tableList.get(i).schema
							.size()];
					Object newRowArray[] = new Object[tableList.get(i).schema
							.size()];

					for (int k = 0; k < tableList.get(i).schema.size(); k++) {

						// k: column number
						// offsetInLocal: row number, locally
						oldRowArray[k] = tableList.get(i).tableContents.get(k)
								.get(offsetInLocal);
						newRowArray[k] = tableList.get(i).updateContents.get(k)
								.get(offsetInUpdateContents);
					}

					SampleRow oldRow = new SampleRow(tableList.get(i).schema,
							oldRowArray);
					SampleRow newRow = new SampleRow(tableList.get(i).schema,
							newRowArray);

					try {
						myDatabase.getStorageInterface().getTableByName(
								tableList.get(i).tableName).updateRow(oldRow,
								newRow);
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}

					// new Row array in same order as schema
					updateRowInLocal(i, oldRowArray, newRowArray);

					Helpers.print("Updated a row by values",
							util.Consts.printType.INFO);

				} else if (operation <= util.Consts.probDeleteByRowID) {

					/*
					 * Delete by row ID
					 */

					int rowID = tableList.get(i).rowIDs.get(offsetInLocal);

					try {
						myDatabase.getStorageInterface().getTableByName(
								tableList.get(i).tableName).deleteRow(rowID);
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}

					removeRowFromTable(i, offsetInLocal);

					Helpers.print("Removed a row by ID",
							util.Consts.printType.INFO);

				} else if (operation <= util.Consts.probDeleteyRowValues) {

					/*
					 * Delete by row values
					 */

					Object rowArray[] = new Object[tableList.get(i).schema
							.size()];

					for (int k = 0; k < tableList.get(i).schema.size(); k++) {

						// k: column number
						// offsetInLocal: row number, locally
						rowArray[k] = tableList.get(i).tableContents.get(k)
								.get(offsetInLocal);

					}

					SampleRow row = new SampleRow(tableList.get(i).schema,
							rowArray);

					try {
						myDatabase.getStorageInterface().getTableByName(
								tableList.get(i).tableName).deleteRow(row);
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}

					// new Row array in same order as schema
					deleteRowInLocal(i, rowArray);

					Helpers.print("Updated a row by values",
							util.Consts.printType.INFO);

				} else if (operation <= util.Consts.probAddNewRow) {

					/*
					 * Add a new row
					 */

					Object rowArray[] = new Object[tableList.get(i).schema
							.size()];

					for (int k = 0; k < tableList.get(i).schema.size(); k++) {

						// k: column number
						// offsetInLocal: row number, locally
						rowArray[k] = tableList.get(i).updateContents.get(k)
								.get(offsetInUpdateContents);

					}

					SampleRow row = new SampleRow(tableList.get(i).schema,
							rowArray);
					int rowID = 0;
					try {
						rowID = myDatabase.getStorageInterface()
								.getTableByName(tableList.get(i).tableName)
								.addRow(row);
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}

					for (int k = 0; k < tableList.get(i).schema.size(); k++) {

						// k: column number
						// offsetInLocal: row number, locally
						tableList.get(i).tableContents.get(k).add(rowArray[k]);
					}
					tableList.get(i).rowIDs.add(new Integer(rowID));

					tableList.get(i).cardinality += 1;

					Helpers
							.print("Added a new Row",
									util.Consts.printType.INFO);

				}

			}

		}

	}

	private static void manipulateTables(Random rand) {

		for (int i = 0; i < currentNumberOfTables; i++) {

			int operation = rand.nextInt(3);

			// remove a column
			if (operation <= 1) {

				if (tableList.get(i).schema.size() <= 1) {
					continue;
				}

				int columnToRemove = rand.nextInt(tableList.get(i).schema
						.size());
				String colName = tableList.get(i).schema.get(columnToRemove)
						.getName();

				try {
					myDatabase.getStorageInterface().getTableByName(
							tableList.get(i).tableName).dropColumnByName(
							colName);
				} catch (NoSuchColumnException e) {
					e.printStackTrace();
				} catch (NoSuchTableException e) {
					e.printStackTrace();
				}

				tableList.get(i).schema.remove(columnToRemove);
				tableList.get(i).mapSchema.remove(colName);
				tableList.get(i).tableContents.remove(columnToRemove);

				Helpers.print("Removed Column " + colName + " from table " + i,
						util.Consts.printType.INFO);

				// might lead to a table with 0 attributes!

			} else if (operation <= 2) {
				// delete a table

				int tableNumberToDelete = rand.nextInt(tableList.size());

				String tableName = tableList.get(tableNumberToDelete).tableName;
				try {
					myDatabase.getStorageInterface().deleteTable(tableName);
				} catch (NoSuchTableException e) {
					e.printStackTrace();
				}
				currentNumberOfTables -= 1;

				tableList.remove(tableNumberToDelete);

				Helpers.print("Removed table " + tableName + " from DB ",
						util.Consts.printType.INFO);

			}

		}

	}

	private static void removeRowFromTable(int tableNumber, int rowOffset) {

		tableList.get(tableNumber).rowIDs.remove(rowOffset);
		for (int k = 0; k < tableList.get(tableNumber).schema.size(); k++) {

			tableList.get(tableNumber).tableContents.get(k).remove(rowOffset);

		}

		tableList.get(tableNumber).cardinality -= 1;
	}

	/**
	 * 
	 */
	public static void test() {

		long time = System.currentTimeMillis();

		/** *************************************************** */
		/* initialize test tables */
		initializeLocalTables();
		/** *************************************************** */

		/** *************************************************** */
		/* Start random data generation for tables */
		generateRandomTables();

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Init DB
		 */

		myDatabase.startSystem();

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Table Creation
		 */
		createTablesInDB();

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Fill tables with all generated data Should do some timing Here
		 */
		fillDBTables();
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * test for correctness
		 */
		if (checkCorrectness()) {

			Helpers.print("All correct", util.Consts.printType.INFO);
		}
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Perform some delete/update
		 */
		manipulateContents(rand);
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * test for correctness
		 */
		if (checkCorrectness()) {

			Helpers.print("All correct", util.Consts.printType.INFO);
		}
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Manipulate tables
		 */
		manipulateTables(rand);
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Test predicate-based getRows
		 */

		/** *************************************************** */

		/** *************************************************** */
		/*
		 * test for correctness
		 */
		if (checkCorrectness()) {

			util.Helpers.print("All correct", util.Consts.printType.INFO);
		}
		/** *************************************************** */

		/** *************************************************** */
		/*
		 * Database out
		 */
		myDatabase.shutdownSystem();

		/** *************************************************** */
		long newTime = System.currentTimeMillis();

		System.out.println(newTime - time);
	}

	private static void updateRowInLocal(int tableNumber, int offsetInLocal,
			Object[] newRowArray) {

		if (newRowArray.length != tableList.get(tableNumber).schema.size()) {

			throw new RuntimeException();

		}

		for (int j = 0; j < tableList.get(tableNumber).schema.size(); j++) {

			// j: col #
			tableList.get(tableNumber).tableContents.get(j).set(offsetInLocal,
					newRowArray[j]);
		}

	}

	/**
	 * 
	 * 
	 * @param tableNumber
	 * @param offsetInLocal
	 * @param oldRowArray
	 * @param newRowArray
	 */
	private static void updateRowInLocal(int tableNumber, Object[] oldRowArray,
			Object[] newRowArray) {

		if (oldRowArray.length != tableList.get(tableNumber).schema.size()
				|| newRowArray.length != tableList.get(tableNumber).schema
						.size()) {

			throw new RuntimeException();

		}

		for (int i = 0; i < tableList.get(tableNumber).cardinality; i++) {

			boolean mismatchFound = false;

			for (int j = 0; j < tableList.get(tableNumber).schema.size(); j++) {

				// i:row
				// j:column
				if (!tableList.get(tableNumber).tableContents.get(j).get(i)
						.equals(oldRowArray[j])) {
					mismatchFound = true;
				}
			}

			if (!mismatchFound) {
				updateRowInLocal(tableNumber, i, newRowArray);
			}
		}

	}
}
