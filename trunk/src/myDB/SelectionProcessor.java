package myDB;

import operator.Operator;
import relationalalgebra.Selection;
import systeminterface.IndexLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import exceptions.SchemaMismatchException;

public class SelectionProcessor extends Processor {

	public SelectionProcessor(IndexLayer iLayer, StorageLayer sLayer) {
		super(iLayer, sLayer);
		// TODO Auto-generated constructor stub
	}

	public String explain(Selection query)
			throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}

	public Operator<? extends Row> query(Selection query)
			throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}

}
