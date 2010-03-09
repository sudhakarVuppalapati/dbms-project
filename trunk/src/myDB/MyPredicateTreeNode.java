package myDB;

import exceptions.IsLeafException;
import exceptions.NotLeafNodeException;
import systeminterface.PredicateTreeNode;
import util.ComparisonOperator;
import util.LogicalOperator;

/**
 * 
 * @author attran
 *
 */
public class MyPredicateTreeNode implements PredicateTreeNode {

	private PredicateTreeNode leftChild;

	private PredicateTreeNode rightChild;

	private LogicalOperator logicalOperator;	
	
	private String columnName;

	private ComparisonOperator comparisonOperator;

	private Object value;
	
	private boolean isLeaf;
	
	public MyPredicateTreeNode() {
		
	}
	
	public MyPredicateTreeNode(String columnName,
			ComparisonOperator comparisonOperator, Object value) {

		this.columnName = columnName;
		this.comparisonOperator = comparisonOperator;
		this.value = value;
		this.isLeaf = true;
	}
	
	public MyPredicateTreeNode(PredicateTreeNode leftChild,
			LogicalOperator logicalOperator, PredicateTreeNode rightChild) {

		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.logicalOperator = logicalOperator;
		this.isLeaf = false;
	}
	
	@Override
	public String getColumnName() throws NotLeafNodeException {
		return columnName;
	}

	@Override
	public ComparisonOperator getComparisonOperator()
			throws NotLeafNodeException {
		return comparisonOperator;
	}

	@Override
	public PredicateTreeNode getLeftChild() throws IsLeafException {
		return leftChild;
	}

	@Override
	public LogicalOperator getLogicalOperator() throws IsLeafException {
		return logicalOperator;
	}

	@Override
	public PredicateTreeNode getRightChild() throws IsLeafException {
		return rightChild;
	}

	@Override
	public Object getValue() throws NotLeafNodeException {
		return value;
	}

	@Override
	public boolean isLeaf() {
		return isLeaf;
	}
	
	protected PredicateTreeNode addConjunct(PredicateTreeNode treeNode, LogicalOperator logicOp) 
	throws NotLeafNodeException {
		if (leftChild == null) {
			leftChild = treeNode;
			columnName = treeNode.getColumnName();
			comparisonOperator = treeNode.getComparisonOperator();
			value = treeNode.getValue();
			isLeaf = true;
			return this;
		}
		else if (rightChild == null) {
			isLeaf = false;
			rightChild = treeNode;
			logicalOperator = logicOp;
			return this;
		}
		else {
			MyPredicateTreeNode newNode = new MyPredicateTreeNode(this,logicOp, treeNode);
			return newNode;
		}
		
	}

}
