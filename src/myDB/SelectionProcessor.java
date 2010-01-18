package myDB;

import operator.Operator;
import relationalalgebra.Selection;
import systeminterface.IndexLayer;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.StorageLayer;
import util.ComparisonOperator;
import exceptions.NoSuchTableException;
import exceptions.NotLeafNodeException;
import exceptions.SchemaMismatchException;

public class SelectionProcessor {

	public static String explain(Selection query, IndexLayer iLayer, StorageLayer sLayer)
			throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}

	public static Operator<? extends Row> query(Selection query, IndexLayer iLayer, StorageLayer sLayer)
			throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static int[] queryAtLeaf(PredicateTreeNode node, String relationName, 
			IndexLayer indexLayer, StorageLayer storageLayer)
	throws NotLeafNodeException, NoSuchTableException, SchemaMismatchException {
		//TODO I am not sure in context of main-memory, using index is always 
		//superior to full scanning
		/** Look for the appropriate index */
		String colName = node.getColumnName();
		ComparisonOperator co = node.getComparisonOperator();
		String[] indexNames = indexLayer.findIndex(relationName, colName);
		
		if (indexNames != null) {
			
		}
		else {
			
		}
		return null;
	}

}
