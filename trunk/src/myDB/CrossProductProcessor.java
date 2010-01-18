package myDB;

import operator.Operator;
import relationalalgebra.CrossProduct;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class CrossProductProcessor extends Processor {
	public CrossProductProcessor(IndexLayer iLayer, StorageLayer sLayer) {
		super(iLayer, sLayer);
		// TODO Auto-generated constructor stub
	}

	public Operator<? extends Row> query(CrossProduct query)
	throws SchemaMismatchException {
		return null;
	}
	
	public String explain(CrossProduct query)
	throws SchemaMismatchException {
		return null;
	}

}
