package util;

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

	protected static final boolean warnMsg = true;

	protected static final boolean errorMsg = true;

	/* Rand seed, to get repeatable experiments */
	public static final long seed = 81;

	/* Schema generation params */

	public static final int schemaMinDim = 1;

	public static final int schemaMaxDim = 7;

	/* String types */
	public static char chars[] = (new String("abcdefghijklmnopqrstuvwxyz12345"))
			.toCharArray();

	/* CHAR type */
	public static final int minCharLength = 1;

	public static final int maxCharLength = 10;

	/* VARCHAR type */
	public static final int minVarcharLength = 1;

	public static final int maxVarcharLength = 5;

	/* Params for rand Date -- see getRandDate() */

	/* Table cardinality */
	public static final int minCardinality = 10;

	public static final int maxCardinality = 50;

	/* Number of tables to create */
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

	/**************************************************************/
	/**************** For 2nd Phase *******************************/

	public static final int minEntriesPerKey = 1;
	/* Max # of row IDs per key */
	public static final int maxEntriesPerKey = 3;

	public static final int minNumKeys = 100;
	public static final int maxNumKeys = 500;

}
