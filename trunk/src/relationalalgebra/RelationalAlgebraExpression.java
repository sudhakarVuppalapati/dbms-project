/**
 * 
 */
package relationalalgebra;

import util.RelationalOperatorType;

/**
 * An abstract relational algebra expression.
 * 
 */
public abstract class RelationalAlgebraExpression {

	private RelationalOperatorType operatorType;

	/**
	 * @param operatorType
	 *            The type of the operator.
	 */
	public RelationalAlgebraExpression(RelationalOperatorType operatorType) {

		this.operatorType = operatorType;
	}

	/**
	 * @return The type of the operator
	 */
	public RelationalOperatorType getType() {

		return this.operatorType;
	}


}
