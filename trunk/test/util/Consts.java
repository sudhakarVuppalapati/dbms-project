package util;

import java.util.Random;

/**
 * 
 * Constants used for testing
 * 
 * @author myahya
 * 
 */
public class Consts {

	/* set what kind of printing you want */
	protected static final boolean infoMsg = false;

	protected static final boolean warnMsg = false;

	protected static final boolean errorMsg = true;

	/** Rand seed, to get repeatable experiments */
	public static final long seed = 81;

	/* Schema generation params */

	/** Min # of attributes in schema */
	public static final int schemaMinDim = 1;
	/** Max # of attributes in schema */
	public static final int schemaMaxDim = 7;

	/* String types */
	/** characters for random string generation */
	public static char chars[] = (new String("abcdefghijklmnopqrstuvwxyz12345"))
			.toCharArray();

	/* CHAR type */
	/** Min CHAR length */
	public static final int minCharLength = 1;
	/** Max CHAR length */
	public static final int maxCharLength = 10;

	/* VARCHAR type */
	/** Min VARCHAR length */
	public static final int minVarcharLength = 1;
	/** Max CHAR length */
	public static final int maxVarcharLength = 5;

	/* Params for rand Date -- see getRandDate() */

	/** Min Table cardinality */
	public static final int minCardinality = 10;
	/** Max Table cardinality */
	public static final int maxCardinality = 50;

	/** Number of tables to create */
	public static final int numTables = 100;

	public enum printType {

		INFO, WARN, ERROR
	}

	/* Probabilities (0-10) */
	public static final int probUpdateByRowID = 2;

	public static final int probUpdateByRowValues = 4;

	public static final int probDeleteByRowID = 6;

	public static final int probDeleteyRowValues = 8;

	public static final int probAddNewRow = 10;

	/* Percentage of table to manipulate */
	public static final double percentManipulate = 0.10;

	public enum DataTypes {
		T_CHAR, T_DATE, T_DOUBLE, T_FLOAT, T_INT, T_LONG, T_VARCHAR
	}

	/** *********************************************************** */
	/** ************** For 2nd Phase ****************************** */

	/** Minimum # payloads per key */
	public static final int minEntriesPerKey = new Random().nextInt(10);
	/** Maximum # payloads per key */
	public static final int maxEntriesPerKey = new Random().nextInt(15) + minEntriesPerKey; //3; //Doesn't make too much sense since it only insert new payloads

	/** Min # unique keys in index */
	public static final int minNumKeys = 100;
	/** Max # unique keys in index */
	public static final int maxNumKeys = 500;

	/** Number of inserts in index at a time */
	public static final int numIndexInserts = new Random().nextInt(3000); //988
	/** Number of point deletes from index at a time */
	public static final int numIndexPointDeletes = 1;
	/** Number of Key deletes from index at a time */
	public static final int numIndexKeyDeletes = 1;
	/** Number of range deletes from index at a time */
	public static final int numIndexRangeDeletes = 1;

	/** Number of point queries to perform on an index at a time */
	public static final int numIndexPointQueries = 10;
	/** Number of range queries to perform on an index at a time */
	public static final int numIndexRangeQueries = 3;
}
