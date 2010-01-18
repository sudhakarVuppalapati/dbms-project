package myDB;

import operator.Operator;
import relationalalgebra.RelationalAlgebraExpression;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;

public class Evaluator {
	
	/** Reference to single evaluators */
	/*private final JoinProcessor join;
	private final ProjectionProcessor projection;
	private final SelectionProcessor selection;
	private final CrossProductProcessor crossProduct;*/
	
	public Evaluator(IndexLayer iLayer, StorageLayer sLayer) {
		/*join = new JoinProcessor(iLayer, sLayer);
		projection = new ProjectionProcessor(iLayer, sLayer);
		selection = new SelectionProcessor(iLayer, sLayer);
		crossProduct = new CrossProductProcessor(iLayer, sLayer);*/
	}
	
	public Operator<? extends Row> eval(RelationalAlgebraExpression query)
	throws NoSuchTableException, SchemaMismatchException,
	NoSuchColumnException, InvalidPredicateException {
		return null;
	}
	
	public String explain(RelationalAlgebraExpression query)
	throws NoSuchTableException, SchemaMismatchException,
	NoSuchColumnException, InvalidPredicateException {
		return null;
	}
}
