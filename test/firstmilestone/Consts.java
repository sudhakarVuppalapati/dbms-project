package firstmilestone;

/**
 * 
 * Constants used for testing
 * 
 * @author myahya
 * 
 */
public class Consts {

	/* set what kind of printing you want */
	protected static final boolean infoMsg = true;
	protected static final boolean warnMsg = true;
	protected static final boolean errorMsg = true;

	/* Rand seed, to get repeatable experiments */
	protected static final long seed = 81;

	/* Schema generation params */

	protected static final int schemaMinDim = 1;
	protected static final int schemaMaxDim = 5;

	/* String types */
	protected static char chars[] = (new String(
			"abcdefghijklmnopqrstuvwxyz12345")).toCharArray();

	/* CHAR type */
	protected static final int minCharLength = 1;
	protected static final int maxCharLength = 10;

	/* VARCHAR type */
	protected static final int minVarcharLength = 1;
	protected static final int maxVarcharLength = 5;

	/* Params for rand Date -- see getRandDate() */

	/* Table cardinality */
	protected static final int minCardinality = 10;
	protected static final int maxCardinality = 50;

	/* Number of tables to create */
	protected static final int numTables = 100;

	protected enum printType {

		INFO, WARN, ERROR
	}

	/* Probabilities (0-10) */
	protected static final int probUpdateByRowID = 2;
	protected static final int probUpdateByRowValues = 4;
	protected static final int probDeleteByRowID = 6;
	protected static final int probDeleteyRowValues = 8;
	protected static final int probAddNewRow = 10;

	/* Percentage of table to manipulate */
	protected static final double percentManipulate = 0.10;

}
