/**
 * 
 */
package relationalalgebra;

import util.RelationalOperatorType;

/**
 * A cross-product takes two relations: L and R and produces a relation whose
 * schema contains all fields of L and all fields of R.
 * 
 */
public class CrossProduct extends RelationalAlgebraExpression {

	private RelationalAlgebraExpression left, right;

	/**
	 * Cross-product constructor
	 * 
	 * @param left
	 *            The left input to the cross-product
	 * @param right
	 *            The right input to the cross-product
	 */
	public CrossProduct(RelationalAlgebraExpression left,
			RelationalAlgebraExpression right) {

		super(RelationalOperatorType.CROSS_PRODUCT);

		if (left == null || right == null) {

			throw new IllegalArgumentException(
					"Cannot pass null inputs to cross product operator");
		}

		this.left = left;
		this.right = right;
	}

	/**
	 * @return The left input of the cross-product
	 */
	public RelationalAlgebraExpression getLeftInput() {

		return this.left;

	}

	/**
	 * @return The right input of the cross-product
	 */
	public RelationalAlgebraExpression getRightInput() {

		return this.right;

	}

}
