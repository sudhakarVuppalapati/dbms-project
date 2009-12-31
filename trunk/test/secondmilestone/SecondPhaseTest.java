package secondmilestone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import metadata.Type;
import metadata.Types;

import org.apache.commons.collections.map.MultiValueMap;

import systeminterface.Database;
import util.Helpers;
import exceptions.IndexAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidRangeException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * Second phase speed test.
 * 
 * MultiValueMap implementation needed to run this test. For that, you need to
 * add lib/commons-collections-3.2.1.jar to your class path. Please see
 * http://commons.apache.org/collections/ for more.
 * 
 * 
 */
public class SecondPhaseTest {

	/** Local indexes */
	private static final HashMap<String, MultiValueMap> indexes = new HashMap<String, MultiValueMap>();

	/** Which index supports range queries */
	private static final HashMap<String, Boolean> supportsRangeQueries = new HashMap<String, Boolean>();

	/** DB */
	private static final Database myDatabase = Database.getInstance();

	/** Table Name */
	private static final String tName = "myTable";

	/** Table attributes */
	private static final String tAttributes[] = { "char_att", "date_att",
			"double_att", "float_att", "int_att", "long_att", "varchar_att" };

	/** Indexes _r: supports range q, _nor: does not */
	private static final String indexNames[] = { "char_att_index_r",
			"char_att_index_nor", "date_att_index_r", "date_att_index_nor",
			"double_att_index_r", "double_att_index_nor", "float_att_index_r",
			"float_att_index_nor", "int_att_index_r", "int_att_index_nor",
			"long_att_index_r", "long_att_index_nor", "varchar_att_index_r",
			"varchar_att_index_nor" };

	/** Random number generator -- add seed when repeatable experiments wanted */
	private static final Random rand = new Random(81);

	/**
	 * Represents a payload along with a flag indicating whether this particular
	 * payload is in the index
	 * 
	 */
	private static class Payload implements Comparable<Object> {

		private int rowID;

		/** Flag indicating whether this payload is inserted in index */
		private boolean inserted;

		Payload(int rowID) {
			this.rowID = rowID;
			this.inserted = false;
		}

		/**
		 * Mark as inserted
		 */
		public void markInserted() {
			this.inserted = true;
		}

		/**
		 * Mark as deleted
		 */
		public void markDeleted() {
			this.inserted = false;
		}

		/**
		 * @return RowID
		 */
		public int getRowID() {
			return this.rowID;
		}

		/**
		 * @return boolean
		 */
		public boolean isInserted() {
			return this.inserted;
		}

		@Override
		public int compareTo(Object o) {
			if (o == null) {

				throw new NullPointerException();
			}
			if (!(o instanceof Payload)) {
				throw new ClassCastException();
			}

			return this.rowID - ((Payload) o).getRowID();
		}

	}

	/**
	 * Create indexes
	 */
	private static void createIndexes() {

		/*
		 * Create underlying tables first
		 */
		HashMap<String, Type> tableSchema = new HashMap<String, Type>();
		tableSchema.put(tAttributes[0], Types
				.getCharType(util.Consts.maxCharLength));
		tableSchema.put(tAttributes[1], Types.getDateType());
		tableSchema.put(tAttributes[2], Types.getDoubleType());
		tableSchema.put(tAttributes[3], Types.getFloatType());
		tableSchema.put(tAttributes[4], Types.getIntegerType());
		tableSchema.put(tAttributes[5], Types.getLongType());
		tableSchema.put(tAttributes[6], Types.getVarcharType());

		try {
			myDatabase.getQueryInterface().createTable(tName, tableSchema);
		} catch (TableAlreadyExistsException e) {
			e.printStackTrace();
		}

		/*
		 * Create indexes in table
		 */

		try {

			/*
			 * create 2 indexes on each attribute, one supporting range queries
			 * and one not
			 */
			for (int i = 0; i < tAttributes.length; i++) {

				myDatabase.getIndexInterface().createIndex(indexNames[(2 * i)],
						tName, tAttributes[i], true);
				myDatabase.getIndexInterface().createIndex(
						indexNames[(2 * i) + 1], tName, tAttributes[i], false);

				/*
				 * Reflect in local info, note use of tree set to avoid
				 * duplicates for same key
				 */

				indexes.put(indexNames[(2 * i)], MultiValueMap.decorate(
						new HashMap<Object, Payload>(), (TreeSet.class)));
				indexes.put(indexNames[(2 * i) + 1], MultiValueMap.decorate(
						new HashMap<Object, Payload>(), (TreeSet.class)));

				supportsRangeQueries.put(indexNames[2 * i], true);
				supportsRangeQueries.put(indexNames[(2 * i) + 1], false);

			}

		} catch (IndexAlreadyExistsException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (SchemaMismatchException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (NoSuchTableException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	/**
	 * Fill local index with values, initially all values marked as "not
	 * inserted"
	 */
	private static void createLocalIndexData() {

		/*
		 * Populate local indexes one by one
		 */

		for (int i = 0; i < tAttributes.length; i++) {

			/*
			 * Max since generated list could have duplicates
			 * 
			 * First, generate all keys for this type
			 */
			int numMaxKeys = rand.nextInt(util.Consts.maxNumKeys
					- util.Consts.minNumKeys + 1)
					+ util.Consts.minNumKeys;

			TreeSet<Object> keys = null;

			if (tAttributes[i].equals("char_att")) {

				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandCharList(rand, numMaxKeys,
								util.Consts.maxCharLength));

			} else if (tAttributes[i].equals("date_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandDateList(rand, numMaxKeys));

			} else if (tAttributes[i].equals("double_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandDoubleList(rand, numMaxKeys));

			} else if (tAttributes[i].equals("float_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandFloatList(rand, numMaxKeys));

			} else if (tAttributes[i].equals("int_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandIntegerList(rand, numMaxKeys));

			} else if (tAttributes[i].equals("long_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandLongList(rand, numMaxKeys));

			} else if (tAttributes[i].equals("varchar_att")) {
				keys = new TreeSet<Object>(util.RandomInputGenerator
						.getRandVarcharList(rand, numMaxKeys));

			}

			assert (keys != null);

			/*
			 * Secondly, for each key, generate payloads and insert each
			 * key-payload pair in local index
			 */
			Iterator<Object> keysIT = keys.iterator();

			while (keysIT.hasNext()) {

				Object key = keysIT.next();

				int numPayloadsForKey = rand
						.nextInt(util.Consts.maxEntriesPerKey
								- util.Consts.minEntriesPerKey + 1)
						+ util.Consts.minEntriesPerKey;

				// actually, treeset of Integer, will cast later
				TreeSet<Object> payloads = new TreeSet<Object>(
						util.RandomInputGenerator.getRandIntegerList(rand,
								numPayloadsForKey));

				Iterator<Object> payloadsIT = payloads.iterator();
				while (payloadsIT.hasNext()) {

					int payload = ((Integer) (payloadsIT.next())).intValue();


					/*
					 * Reflect locally
					 */

					indexes.get(indexNames[(2 * i)]).put(key,
							new Payload(payload));
					indexes.get(indexNames[(2 * i) + 1]).put(key,
							new Payload(payload));
				}

			}

		}
	}

	/**
	 * Print local indexes, for each payload prints whether this payload is
	 * inserted in the index or not
	 */
	private static void printLocalIndexData() {

		for (int indexNum = 0; indexNum < indexNames.length; indexNum++) {

			String indexName = indexNames[indexNum];
			System.out
					.println("******************************************************");
			System.out.println(indexName + "::");

			MultiValueMap localIndexMap = indexes.get(indexName);

			ArrayList<Comparable<Object>> keys = new ArrayList<Comparable<Object>>(
					localIndexMap.keySet());

			Collections.sort(keys);

			for (int i = 0; i < keys.size(); i++) {

				System.out.println(keys.get(i));

				TreeSet<Payload> payloadSet = new TreeSet<Payload>(
						localIndexMap.getCollection(keys.get(i)));

				Iterator<Payload> payloadIT = payloadSet.iterator();

				while (payloadIT.hasNext()) {

					Payload payload = payloadIT.next();
					
					System.out.println("\t" + payload.getRowID() + ":"
							+ payload.isInserted());

				}

			}

		}

	}

	/**
	 * Manipulate indexes: insert/delete/update
	 */
	private static void manipulateIndexes() {

		/*
		 * Insert entries
		 */

		insertEntries();

		/*
		 * Delete entries
		 */
		deleteEntries();
	}

	/**
	 * Insert entries into indexes
	 * 
	 */
	private static void insertEntries() {

		for (int indexNum = 0; indexNum < indexNames.length; indexNum++) {

			for (int insertionNumber = 0; insertionNumber < util.Consts.numIndexInserts; insertionNumber++) {

				/*
				 * Get a random key for this index
				 */

				Object randKey = getRandKey(indexNum);

				if (randKey != null) {

					/*
					 * try maxEntriesPerKey times to get a payload that was not
					 * inserted before for randKey
					 */
					Payload payload = null;
					for (int i = 0; i < util.Consts.maxEntriesPerKey; i++) {

						payload = getRandPayloadForKey(randKey, indexNum);
						if (!payload.isInserted()) {

							/*
							 * This payload is not already in the index -> add
							 * it
							 */
							String indexName = indexNames[indexNum];
							try {
								myDatabase.getIndexInterface().insertIntoIndex(
										indexName, randKey, payload.getRowID());
								payload.markInserted();

								Helpers.print("New Entry Inserted",
										util.Consts.printType.INFO);
							} catch (NoSuchIndexException e) {
								e.printStackTrace();
							} catch (InvalidKeyException e) {
								e.printStackTrace();
							}

							break;

						}

					}

				}
			}

		}

	}

	private static void deleteEntries() {

		for (int indexNum = 0; indexNum < indexNames.length; indexNum++) {

			/*
			 * Delete a single entry
			 */
			for (int pointDeletionNumber = 0; pointDeletionNumber < util.Consts.numIndexPointDeletes; pointDeletionNumber++) {

				deleteSingleEntryFromIndex(indexNum);
			}

			/*
			 * Delete all entries for a key
			 */
			for (int keyDeletionNumber = 0; keyDeletionNumber < util.Consts.numIndexKeyDeletes; keyDeletionNumber++) {
				deleteAllEntriesForKeyFromIndex(indexNum);
			}
			/*
			 * Delete a range, only for index supporting range
			 */

			if (supportsRangeQueries.get(indexNames[indexNum]).booleanValue()) {
				for (int rangeDeletionNumber = 0; rangeDeletionNumber < util.Consts.numIndexRangeDeletes; rangeDeletionNumber++) {
					deleteRangeFromIndex(indexNum);
				}
			}

		}

	}

	/**
	 * Delete a single randomly chosen key-value pair from index
	 * 
	 * @param indexNum
	 *            index number
	 */
	private static void deleteSingleEntryFromIndex(int indexNum) {

		Object randKey = getRandKey(indexNum);

		if (randKey != null) {

			Payload payload = null;
			/*
			 * try maxEntriesPerKey times to get a payload that was inserted
			 * before for randKey
			 */
			for (int i = 0; i < util.Consts.maxEntriesPerKey; i++) {

				payload = getRandPayloadForKey(randKey, indexNum);
				if (payload.isInserted()) {

					/*
					 * This payload is already in the index -> delete it
					 */
					String indexName = indexNames[indexNum];
					try {
						myDatabase.getIndexInterface().deleteFromIndex(
								indexName, randKey, payload.getRowID());
						payload.markDeleted();
						Helpers.print("Point deletion done",
								util.Consts.printType.INFO);
					} catch (NoSuchIndexException e) {
						e.printStackTrace();
					} catch (InvalidKeyException e) {
						e.printStackTrace();
					}

					break;

				}

			}

		}

	}

	/**
	 * Delete all entries for a randomly chosen key
	 * 
	 * @param indexNum
	 *            index number
	 */
	private static void deleteAllEntriesForKeyFromIndex(int indexNum) {

		Object randKey = getRandKey(indexNum);

		if (randKey != null) {

			String indexName = indexNames[indexNum];
			try {
				/*
				 * Perform index deletion
				 */
				myDatabase.getIndexInterface().deleteFromIndex(indexName,
						randKey);

				/*
				 * Mark all local entries as deleted
				 */

				TreeSet<Payload> payloadSet = new TreeSet<Payload>(indexes.get(
						indexName).getCollection(randKey));
				Iterator<Payload> payloadIT = payloadSet.iterator();
				while (payloadIT.hasNext()) {
					payloadIT.next().markDeleted();
					Helpers.print("Key deletion has effect",
							util.Consts.printType.INFO);
				}

			} catch (NoSuchIndexException e) {
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * Delete a range of keys
	 * 
	 * @param indexNum
	 *            index number
	 */
	private static void deleteRangeFromIndex(int indexNum) {

		Object randStartKey = getRandKey(indexNum);
		Object randEndKey = getRandKey(indexNum);

		if (randStartKey == null || randEndKey == null) {
			return;
		}

		/*
		 * Range should make sense
		 */
		if (((Comparable<Object>) randStartKey).compareTo(randEndKey) > 0) {

			Object temp = randStartKey;
			randStartKey = randEndKey;
			randEndKey = temp;
		}

		String indexName = indexNames[indexNum];

		try {

			/*
			 * Delete range from index
			 */
			myDatabase.getIndexInterface().deleteFromIndex(indexName,
					randStartKey, randEndKey);

			/*
			 * reflect locally
			 */

			ArrayList<Comparable<Object>> keys = new ArrayList<Comparable<Object>>(
					indexes.get(indexName).keySet());

			for (int i = 0; i < keys.size(); i++) {

				/*
				 * In range?
				 */

				if (keys.get(i).compareTo(randStartKey) >= 0
						&& keys.get(i).compareTo(randEndKey) <= 0) {

					/*
					 * Mark all entries for the key in range as deleted
					 */
					TreeSet<Payload> payloadSet = new TreeSet<Payload>(indexes
							.get(indexName).getCollection(keys.get(i)));
					Iterator<Payload> payloadIT = payloadSet.iterator();
					while (payloadIT.hasNext()) {
						payloadIT.next().markDeleted();
						Helpers.print("Range deletion has effect",
								util.Consts.printType.INFO);
					}

				}

			}

		} catch (NoSuchIndexException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			e.printStackTrace();
		} catch (RangeQueryNotSupportedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Return one row ID for they key in the index
	 * 
	 * @param key
	 * @param tAttributeOffset
	 * @return a rowID
	 */
	@SuppressWarnings("unchecked")
	private static Payload getRandPayloadForKey(Object key, int indexOffset) {

		TreeSet<Payload> payloadSet = new TreeSet<Payload>(indexes.get(
				indexNames[indexOffset]).getCollection(key));

		int keyOffset = rand.nextInt(payloadSet.size());

		Iterator<Payload> objectIT = payloadSet.iterator();

		for (int i = 0; i < keyOffset; i++, objectIT.next())
			;
		return objectIT.next();

	}

	/**
	 * Return some random key from one of the two indexes on tAttributeOffset
	 * 
	 * @param tAttributeOffset
	 * @return An Object = random key
	 */
	@SuppressWarnings("unchecked")
	private static Object getRandKey(int indexOffset) {

		Set<Object> keySet = indexes.get(indexNames[indexOffset]).keySet();

		if (keySet.size() == 0)
			return null;

		int keyOffset = rand.nextInt(keySet.size());

		Iterator<Object> objectIT = keySet.iterator();

		for (int i = 0; i < keyOffset; i++, objectIT.next())
			;

		Object ret = objectIT.next();

		if (indexes.get(indexNames[indexOffset]).getCollection(ret).size() == 0)
			return null;
		else
			return ret;

	}

	/**
	 * Perform range & point queries
	 */
	private static void performQueries() {

		for (int indexNum = 0; indexNum < indexNames.length; indexNum++) {

			/*
			 * Perform point queries
			 */

			for (int pointQueryNumber = 0; pointQueryNumber < util.Consts.numIndexPointQueries; pointQueryNumber++) {
				performPointQuery(indexNum);
			}

			/*
			 * Perform range queries
			 */
			if (supportsRangeQueries.get(indexNames[indexNum]).booleanValue()) {
				for (int rangeQueryNumber = 0; rangeQueryNumber < util.Consts.numIndexRangeQueries; rangeQueryNumber++) {

					performRangeQuery(indexNum);
				}
			}

		}

	}

	/**
	 * Perform a range query. Check result with local index
	 * 
	 * @param indexNum
	 */
	private static void performRangeQuery(int indexNum) {

		Object randStartKey = getRandKey(indexNum);
		Object randEndKey = getRandKey(indexNum);

		if (randStartKey == null || randEndKey == null) {
			return;
		}

		/*
		 * Range should make sense
		 */
		if (((Comparable<Object>) randStartKey).compareTo(randEndKey) > 0) {

			Object temp = randStartKey;
			randStartKey = randEndKey;
			randEndKey = temp;
		}

		String indexName = indexNames[indexNum];

		/*
		 * Get entries from index
		 */
		int[] queryResult = null;

		try {
			queryResult = myDatabase.getIndexInterface().rangeQueryRowIDs(
					indexName, randStartKey, randEndKey);

		} catch (NoSuchIndexException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			e.printStackTrace();
		} catch (RangeQueryNotSupportedException e) {
			e.printStackTrace();
		}

		TreeSet<Integer> resultSet = new TreeSet<Integer>();
		for (int i = 0; i < queryResult.length; i++) {
			resultSet.add(queryResult[i]);
		}

		/*
		 * Get entries for local index
		 */

		TreeSet<Integer> expectedResultSet = new TreeSet<Integer>();

		MultiValueMap localIndexMap = indexes.get(indexName);

		ArrayList<Comparable<Object>> keys = new ArrayList<Comparable<Object>>(
				localIndexMap.keySet());

		Collections.sort(keys);

		/*
		 * index of the search key, if it is contained in the list; otherwise,
		 * (-(insertion point) - 1) see javadoc for Collections.binarySearch
		 */
		int offset = Collections.binarySearch(keys, randStartKey);

		if (offset < 0) {
			offset = -1 * (offset + 1);
		}

		while (offset < keys.size()) {

			if (inRange(keys.get(offset), randStartKey, randEndKey)) {

				/*
				 * Get all payloads for this key marked as inserted
				 */
				TreeSet<Payload> payloadSet = new TreeSet<Payload>(
						localIndexMap.getCollection(keys.get(offset)));

				Iterator<Payload> payloadIT = payloadSet.iterator();

				while (payloadIT.hasNext()) {
					Payload p = payloadIT.next();
					if (p.isInserted()) {
						Helpers.print("An inserted value found - r",
								util.Consts.printType.INFO);
						expectedResultSet.add(p.getRowID());
					}
				}

			} else {
				break;
			}

			offset++;

		}

		/*
		 * Check if results from index match local ones
		 */

		if (expectedResultSet.equals(resultSet)) {

			Helpers.print("Correct range query result from index #" + indexNum,
					util.Consts.printType.INFO);

		} else {

			Helpers.print("Incorrect range query result from index #"
					+ indexNum, util.Consts.printType.ERROR);
			System.exit(-1);
		}

	}

	@SuppressWarnings("unchecked")
	private static boolean inRange(Object currentKey, Object startSearchKey,
			Object endSearchKey) {
		return (((Comparable<Object>) currentKey).compareTo(startSearchKey) >= 0)
				&& (((Comparable<Object>) currentKey).compareTo(endSearchKey) <= 0);
	}

	/**
	 * Perform a point query. Check result with local index
	 * 
	 * @param indexNum
	 */
	private static void performPointQuery(int indexNum) {

		/*
		 * Random key
		 */
		Object randKey = getRandKey(indexNum);

		if (randKey != null) {

			/*
			 * Get entries from index
			 */
			String indexName = indexNames[indexNum];
			int[] queryResult = null;

			try {
				queryResult = myDatabase.getIndexInterface().pointQueryRowIDs(
						indexName, randKey);
			} catch (NoSuchIndexException e) {
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			}

			TreeSet<Integer> resultSet = new TreeSet<Integer>();
			for (int i = 0; i < queryResult.length; i++) {
				resultSet.add(queryResult[i]);
			}

			/*
			 * Get entries for local index
			 */
			TreeSet<Payload> payloadSet = new TreeSet<Payload>(indexes.get(
					indexName).getCollection(randKey));
			TreeSet<Integer> expectedResultSet = new TreeSet<Integer>();
			Iterator<Payload> payloadIT = payloadSet.iterator();

			while (payloadIT.hasNext()) {
				Payload p = payloadIT.next();
				if (p.isInserted()) {
					Helpers.print("An inserted value found",
							util.Consts.printType.INFO);
					expectedResultSet.add(p.getRowID());
				}
			}

			/*
			 * Check if results from index match local ones
			 */

			if (expectedResultSet.equals(resultSet)) {

				Helpers.print("Correct point query result from index #"
						+ indexNum, util.Consts.printType.INFO);

			} else {

				Helpers.print("Incorrect point query result from index #"
						+ indexNum, util.Consts.printType.ERROR);
				System.exit(-1);
			}

		}

	}

	/**
	 * Perform test
	 */
	public static void test() {

		/*
		 * Create Indexes
		 */
		createIndexes();

		/*
		 * Generate data to go into indexes
		 */
		createLocalIndexData();

		/*
		 * print local indexs
		 */
		printLocalIndexData();

		/*
		 * Perform insertions & deletions
		 */
		manipulateIndexes();

		/*
		 * Perform queries, result of each query checked against local indexes
		 */
		performQueries();

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

	}
}
