package myDB;

import operator.Operator;
import relationalalgebra.Join;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class JoinProcessor {

	public static Operator<? extends Row> query(Join query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}
	
	public String explain(Join query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}
}
