package relationalalgebra;

import util.RelationalOperatorType;

/**
 * 
 * An equijoin with the join attributes specified. The join operator takes two
 * input relations L and R and the names of two attributes, one from each
 * relation, on which the equijoin is performed. The schema of the resulting
 * relation has all attributes of L and all attributes of R.
 * 
 */
public class Join extends RelationalAlgebraExpression {

	private RelationalAlgebraExpression left, right;
	private String leftJoinAttribute, rightJoinAttribute;

	/**
	 * @param left
	 *            The left input to the join.
	 * @param right
	 *            The right input to the join.
	 * @param leftJoinAttribute
	 *            The name of the attribute from the left input relation on
	 *            which the join is performed.
	 * @param rightJoinAttribute
	 *            The name of the attribute from the right input relation on
	 *            which the join is performed.
	 */
	public Join(RelationalAlgebraExpression left,
			RelationalAlgebraExpression right, String leftJoinAttribute,
			String rightJoinAttribute) {

		super(RelationalOperatorType.JOIN);

		if (left == null || right == null) {

			throw new IllegalArgumentException(
					"Cannot pass null inputs to join operator");
		}

		if (leftJoinAttribute == null || rightJoinAttribute == null) {

			throw new IllegalArgumentException(
					"Cannot pass null inputs to join operator");
		}

		this.left = left;
		this.right = right;
		this.leftJoinAttribute = leftJoinAttribute;
		this.rightJoinAttribute = rightJoinAttribute;
	}

	/**
	 * @return The left input relation.
	 */
	public RelationalAlgebraExpression getLeftInput() {

		return this.left;
	}

	/**
	 * @return The right input relation.
	 */
	public RelationalAlgebraExpression getRightInput() {

		return this.right;
	}

	/**
	 * @return The name of the attribute from the left input relation on which
	 *         the join is performed.
	 */
	public String getLeftJoinAttribute() {

		return this.leftJoinAttribute;
	}

	/**
	 * @return The name of the attribute from the right input relation on which
	 *         the join is performed.
	 */
	public String getRightJoinAttribute() {

		return this.rightJoinAttribute;
	}

}
