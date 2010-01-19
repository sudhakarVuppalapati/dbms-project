package myDB;

import operator.Operator;
import relationalalgebra.Input;
import relationalalgebra.Selection;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;

public class InputProcessor {
	
	public static String explain(Selection query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static Operator<? extends Row> query(Input query, StorageLayer storageLayer) 
	throws NoSuchTableException {
		String relation = query.getRelationName();
		return (storageLayer.getTableByName(relation)).getRows(); 
	}
	
}
