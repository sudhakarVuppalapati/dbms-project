package myDB;

import operator.Operator;
import relationalalgebra.CrossProduct;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class CrossProductProcessor {
	
	public static Operator<? extends Row> query(CrossProduct query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}
	
	public String explain(CrossProduct query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		return null;
	}

}
