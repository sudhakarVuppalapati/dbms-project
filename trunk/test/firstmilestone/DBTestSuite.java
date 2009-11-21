package firstmilestone;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test Suite
 * 
 * @author Mohamed
 * 
 */
public class DBTestSuite {

	/**
	 * @return test
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("Test for test");
		// $JUnit-BEGIN$
		suite.addTestSuite(StorageLayerTest.class);
		suite.addTestSuite(TableTest.class);

		// $JUnit-END$
		return suite;
	}

	/**
	 * @param args
	 *            args
	 */
	public static void main(String[] args) {

		junit.textui.TestRunner.run(suite());
	}

}
