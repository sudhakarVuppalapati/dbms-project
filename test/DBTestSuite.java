import junit.framework.Test;
import junit.framework.TestSuite;
import secondmilestone.IndexLayerTest;
import secondmilestone.QueryLayerTest;
import thirdmilestone.LogicalOperatorsTest;
import firstmilestone.StorageLayerTest;
import firstmilestone.TableTest;

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
		suite.addTestSuite(IndexLayerTest.class);
		suite.addTestSuite(QueryLayerTest.class);
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
