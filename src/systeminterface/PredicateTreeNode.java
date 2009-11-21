package systeminterface;

import util.ComparisonOperator;
import util.LogicalOperator;
import exceptions.IsLeafException;
import exceptions.NotLeafNodeException;

/**
 * 
 * A node in a predicate tree.
 * 
 * 
 * 
 */
public interface PredicateTreeNode {

	/**
	 * Return left child of this node in predicate tree
	 * 
	 * @return Reference to left child
	 * @throws IsLeafException
	 *             Current node is actually a leaf
	 */

	public PredicateTreeNode getLeftChild() throws IsLeafException;

	/**
	 * Return left child of this node in predicate tree
	 * 
	 * @return Reference to right child
	 * @throws IsLeafException
	 *             Current node is actually a leaf
	 */

	public PredicateTreeNode getRightChild() throws IsLeafException;

	/**
	 * Is current node a leaf (storing [ColumnName ComparisonOperator Value]
	 * 
	 * @return boolean
	 */
	public boolean isLeaf();

	/**
	 * In the case of a node representing a logical operator (AND/OR) return
	 * back this operator
	 * 
	 * @return Logical operator represented by node
	 * @throws IsLeafException
	 *             Current node is actually a leaf
	 */
	public LogicalOperator getLogicalOperator() throws IsLeafException;

	/**
	 * @return The name of the column represented by that node
	 * @throws NotLeafNodeException
	 *             This node is not a leaf node
	 */
	public String getColumnName() throws NotLeafNodeException;

	/**
	 * @return The comparison operator used in the node
	 * @throws NotLeafNodeException
	 *             the node is not a leaf node
	 */
	public ComparisonOperator getComparisonOperator()
			throws NotLeafNodeException;

	/**
	 * @return The value against which the column will be compared
	 * @throws NotLeafNodeException
	 *             This node is not a leaf node
	 */
	public Object getValue() throws NotLeafNodeException;

}
