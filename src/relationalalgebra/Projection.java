package relationalalgebra;

import util.RelationalOperatorType;

/**
 * A projection that takes in an input relation and returns a relation with a
 * schema containing a subset of the input relation's attributes.
 * 
 */
public class Projection extends RelationalAlgebraExpression {

	private String[] projectionAttributes;

	/**
	 * @param projectionAttributes
	 *            An array of the names of the attributes that will be
	 *            projected.
	 * 
	 */
	public Projection(String[] projectionAttributes) {

		super(RelationalOperatorType.PROJECTION);

		if (projectionAttributes == null) {

			throw new IllegalArgumentException(
					"Cannot pass null input to projection operator");
		}

		this.projectionAttributes = projectionAttributes;

	}

	/**
	 * @return The names of the attributes that will be projected
	 */
	public String[] getProjectionAttributes() {

		return this.projectionAttributes;

	}

}
