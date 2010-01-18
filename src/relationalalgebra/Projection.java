package relationalalgebra;

import util.RelationalOperatorType;

/**
 * A projection that takes in an input relation and returns a relation with a
 * schema containing a subset of the input relation's attributes.
 * 
 */
public class Projection extends RelationalAlgebraExpression {

	private RelationalAlgebraExpression input;

	private String[] projectionAttributes;

	/**
	 * @param input 
	 * @param projectionAttributes
	 *            An array of the names of the attributes that will be
	 *            projected.
	 * 
	 */
	public Projection(RelationalAlgebraExpression input,
			String[] projectionAttributes) {

		super(RelationalOperatorType.PROJECTION);

		if (projectionAttributes == null || input == null) {

			throw new IllegalArgumentException(
					"Cannot pass null input to projection operator");
		}
		this.input = input;
		this.projectionAttributes = projectionAttributes;

	}
	
	/**
	 * @return The input relation.
	 */
	public RelationalAlgebraExpression getInput(){

		return this.input;
	
	}

	/**
	 * @return The names of the attributes that will be projected
	 */
	public String[] getProjectionAttributes() {

		return this.projectionAttributes;

	}

}
