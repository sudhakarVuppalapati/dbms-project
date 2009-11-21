package sampleDB;

import systeminterface.PredicateTreeNode;
import util.ComparisonOperator;
import util.LogicalOperator;
import exceptions.IsLeafException;
import exceptions.NotLeafNodeException;

/**
 * 
 * Represents a leaf tree
 * 
 * 
 */
public class LeafPredicateTreeNode implements PredicateTreeNode {

	private final String columnName;
	private final ComparisonOperator comparisonOperator;
	private final Object value;

	/**
	 * 
	 * @param columnName
	 *            Column name
	 * @param comparisonOperator
	 *            comparison Op
	 * @param value
	 *            vaue
	 */
	public LeafPredicateTreeNode(String columnName,
			ComparisonOperator comparisonOperator, Object value) {

		this.columnName = columnName;
		this.comparisonOperator = comparisonOperator;
		this.value = value;

	}

	@Override
	public String getColumnName() throws NotLeafNodeException {
		return this.columnName;
	}

	@Override
	public ComparisonOperator getComparisonOperator()
			throws NotLeafNodeException {
		return this.comparisonOperator;
	}

	@Override
	public PredicateTreeNode getLeftChild() throws IsLeafException {
		throw new IsLeafException();
	}

	@Override
	public LogicalOperator getLogicalOperator() throws IsLeafException {
		throw new IsLeafException();
	}

	@Override
	public PredicateTreeNode getRightChild() throws IsLeafException {
		throw new IsLeafException();
	}

	@Override
	public Object getValue() throws NotLeafNodeException {
		return this.value;
	}

	@Override
	public boolean isLeaf() {
		return true;
	}

}
