/**
 * 
 */
package myDB;

import exceptions.IsLeafException;
import exceptions.NotLeafNodeException;
import systeminterface.PredicateTreeNode;
import util.ComparisonOperator;
import util.LogicalOperator;

/**
 * @author tuanta
 *
 */
public class MyPredicateTreeNode implements PredicateTreeNode {

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getColumnName()
	 */
	@Override
	public String getColumnName() throws NotLeafNodeException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getComparisonOperator()
	 */
	@Override
	public ComparisonOperator getComparisonOperator()
			throws NotLeafNodeException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getLeftChild()
	 */
	@Override
	public PredicateTreeNode getLeftChild() throws IsLeafException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getLogicalOperator()
	 */
	@Override
	public LogicalOperator getLogicalOperator() throws IsLeafException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getRightChild()
	 */
	@Override
	public PredicateTreeNode getRightChild() throws IsLeafException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#getValue()
	 */
	@Override
	public Object getValue() throws NotLeafNodeException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.PredicateTreeNode#isLeaf()
	 */
	@Override
	public boolean isLeaf() {
		// TODO Auto-generated method stub
		return false;
	}

}
