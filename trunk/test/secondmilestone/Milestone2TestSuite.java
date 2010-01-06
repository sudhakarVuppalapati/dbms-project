package secondmilestone;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test Suite
 * 
 * @author Mohamed
 * 
 */
public class Milestone2TestSuite {

	/**
	 * @return test
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("Test for test");
		// $JUnit-BEGIN$
		suite.addTestSuite(IndexLayerTest.class);
		suite.addTestSuite(QueryLayerTest.class);
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
