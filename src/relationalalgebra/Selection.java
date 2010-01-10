/**
 * 
 */
package relationalalgebra;

import systeminterface.PredicateTreeNode;
import util.RelationalOperatorType;

/**
 * A selection that takes relation and a predicate and outputs a relation with
 * tuples satisfying the predicate (in the form of a predicate tree)
 * 
 */
public class Selection extends RelationalAlgebraExpression {

	private PredicateTreeNode predicate;
	private RelationalAlgebraExpression input;

	/**
	 * @param input
	 * @param predicate
	 */
	public Selection(RelationalAlgebraExpression input,
			PredicateTreeNode predicate) {

		super(RelationalOperatorType.SELECTION);

		if (input == null || predicate == null) {

			throw new IllegalArgumentException(
					"Cannot pass null input to selection operator");
		}

		this.input = input;
		this.predicate = predicate;

	}

	/**
	 * @return The input relation.
	 */
	public RelationalAlgebraExpression getInput() {

		return this.input;

	}

	/**
	 * @return The root of the predicate tree
	 */
	public PredicateTreeNode getPredicate() {

		return this.predicate;
	}

}
