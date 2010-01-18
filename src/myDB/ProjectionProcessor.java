package myDB;

import operator.Operator;
import relationalalgebra.Projection;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class ProjectionProcessor extends Processor {
	
	public ProjectionProcessor(IndexLayer iLayer, StorageLayer sLayer) {
		super(iLayer, sLayer);
		// TODO Auto-generated constructor stub
	}

	public Operator<? extends Row> query(Projection query)
	throws SchemaMismatchException {
		return null;
	}
	
	public String explain(Projection query)
	throws SchemaMismatchException {
		return null;
	}
}
