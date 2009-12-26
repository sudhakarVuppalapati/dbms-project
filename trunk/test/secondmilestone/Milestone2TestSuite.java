package secondmilestone;

import junit.framework.Test;
import junit.framework.TestSuite;
import firstmilestone.StorageLayerTest;
import firstmilestone.TableTest;

public class Milestone2TestSuite {

	/**
	 * @return test
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("Test for test");
		// $JUnit-BEGIN$
		suite.addTestSuite(DummyIndexLayerTest.class);
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
