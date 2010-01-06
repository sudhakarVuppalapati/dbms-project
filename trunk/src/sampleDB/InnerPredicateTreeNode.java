/**
 * 
 */
package sampleDB;

import systeminterface.PredicateTreeNode;
import util.ComparisonOperator;
import util.LogicalOperator;
import exceptions.IsLeafException;
import exceptions.NotLeafNodeException;

/**
 * Represents a logical operator node
 */
public class InnerPredicateTreeNode implements PredicateTreeNode {

	private final PredicateTreeNode leftChild;

	private final PredicateTreeNode rightChild;

	private final LogicalOperator logicalOperator;

	/**
	 * @param leftChild
	 *            Left child
	 * @param logicalOperator
	 *            logical op
	 * @param rightChild
	 *            right child
	 */
	public InnerPredicateTreeNode(PredicateTreeNode leftChild,
			LogicalOperator logicalOperator, PredicateTreeNode rightChild) {

		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.logicalOperator = logicalOperator;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getColumnName()
	 */
	@Override
	public String getColumnName() throws NotLeafNodeException {
		throw new NotLeafNodeException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getComparisonOperator()
	 */
	@Override
	public ComparisonOperator getComparisonOperator()
			throws NotLeafNodeException {
		throw new NotLeafNodeException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getLeftChild()
	 */
	@Override
	public PredicateTreeNode getLeftChild() throws IsLeafException {
		return this.leftChild;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getLogicalOperator()
	 */
	@Override
	public LogicalOperator getLogicalOperator() throws IsLeafException {
		return this.logicalOperator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getRightChild()
	 */
	@Override
	public PredicateTreeNode getRightChild() throws IsLeafException {
		return this.rightChild;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#getValue()
	 */
	@Override
	public Object getValue() throws NotLeafNodeException {
		throw new NotLeafNodeException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see systeminterface.PredicateTreeNode#isLeaf()
	 */
	@Override
	public boolean isLeaf() {
		return false;
	}

}
