	package thirdmilestone;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test Suite
 * 
 * @author Mohamed
 * 
 * @version 273
 */
public class Milestone3TestSuite {

	/**
	 * @return test
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("Test for test");
		// $JUnit-BEGIN$
		suite.addTestSuite(LogicalOperatorsTest.class);
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
