package fourthmilestone;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test Suite
 * 
 * @author Mohamed
 * 
 * @version 224
 */
public class Milestone4TestSuite {

	/**
	 * @return test
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("Test for test");
		// $JUnit-BEGIN$
		suite.addTestSuite(TranactionLayerTest.class);
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
