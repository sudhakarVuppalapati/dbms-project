package myDB;

import operator.Operator;
import relationalalgebra.Projection;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class ProjectionProcessor {
	
	public static Operator<? extends Row> query(Projection query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}
	
	public static String explain(Projection query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}
}
