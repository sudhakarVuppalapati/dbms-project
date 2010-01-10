package relationalalgebra;

import util.RelationalOperatorType;

/**
 * An input relation. A relational algebra expression node is an input node iff
 * it is a leaf of the operator tree.
 * 
 */
public class Input extends RelationalAlgebraExpression {

	private String relationName;

	/**
	 * 
	 * @param relationName
	 *            The name of the relation
	 */
	public Input(String relationName) {

		super(RelationalOperatorType.INPUT);

		if (relationName == null) {

			throw new IllegalArgumentException(
					"Cannot pass null input to selection operator");
		}

		this.relationName = relationName;

	}

	/**
	 * @return Returns the name of the relation.
	 */
	public String getRelationName() {
		return this.relationName;
	}

}
