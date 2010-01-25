package myDB.physicaloperators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import metadata.Type;
import metadata.Types;
import myDB.MyColumn;
import myDB.MyDoubleColumn;
import myDB.MyFloatColumn;
import myDB.MyIntColumn;
import myDB.MyLongColumn;
import myDB.MyNull;
import myDB.MyObjectColumn;
import exceptions.InvalidKeyException;
import exceptions.InvalidPredicateException;
import exceptions.InvalidRangeException;
import exceptions.IsLeafException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.NotLeafNodeException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;
import systeminterface.Column;
import systeminterface.IndexLayer;
import systeminterface.PredicateTreeNode;
import util.ComparisonOperator;
import util.LogicalOperator;

public class SelectionOperator {
	
	/**
	 * This method acts as a intercepter to a various types of select methods.
	 * @param data
	 * @param relations
	 * @param predicate
	 * @param indexLayer
	 * @return
	 * @throws InvalidPredicateException
	 * @throws SchemaMismatchException
	 * @throws NoSuchTableException
	 */
	public static Map<String, Column> select(Map<String, Column> data, PredicateTreeNode predicate, 
			String relation, IndexLayer indexLayer)
			throws InvalidPredicateException, SchemaMismatchException, NoSuchTableException {
		return multipleIndexSelect(data, relation, predicate, indexLayer);
	}
	

	private static Map<String, Column> multipleIndexSelect(Map<String, Column> data, String relations, 
			PredicateTreeNode predicate, IndexLayer indexLayer) 
			throws InvalidPredicateException, SchemaMismatchException, NoSuchTableException {

		int[] result;
		if (relations != null) 
				result = singleBoundedSelect(data, relations, predicate, indexLayer, null);
		else 
			result =  boundedSelect(data, predicate, null);
		
		int n = result.length;
		Type type;
		List<Column> colList = new ArrayList<Column>();
		MyColumn tmpCol;
		for (Column col : data.values()) {
			type = col.getColumnType();
			if (type == Types.getIntegerType()) {
				int[] tmpContent = (int[])col.getDataArrayAsObject(); 
				int[] newData = new int[result.length];
				for (int i = 0; i < n; i++) {
					newData[i] = tmpContent[result[i]]; 
				}
				tmpCol = new MyIntColumn(col.getColumnName(), type, newData);
			}
			else if (type == Types.getFloatType()) {
				float[] tmpContent = (float[])col.getDataArrayAsObject(); 
				float[] newData = new float[result.length];
				for (int i = 0; i < n; i++) {
					newData[i] = tmpContent[result[i]]; 
				}
				tmpCol = new MyFloatColumn(col.getColumnName(), type, newData);
			}
			else if (type == Types.getDoubleType()) {
				double[] tmpContent = (double[])col.getDataArrayAsObject(); 
				double[] newData = new double[result.length];
				for (int i = 0; i < n; i++) {
					newData[i] = tmpContent[result[i]]; 
				}
				tmpCol = new MyDoubleColumn(col.getColumnName(), type, newData);
			}
			else if (type == Types.getLongType()) {
				long[] tmpContent = (long[])col.getDataArrayAsObject(); 
				long[] newData = new long[result.length];
				for (int i = 0; i < n; i++) {
					newData[i] = tmpContent[result[i]]; 
				}
				tmpCol = new MyLongColumn(col.getColumnName(), type, newData);
			}
			//THIS IS REFERENCE, NOT COPY
			else {
				Object[] tmpContent = (Object[])col.getDataArrayAsObject(); 	
				List newData = new ArrayList();
				for (int i = 0; i < n; i++) {
					newData.add(tmpContent[result[i]]); 
				}
				tmpCol = new MyObjectColumn(col.getColumnName(), type, newData);
			
			}
			colList.add(tmpCol);
		}
		
		//Possible optimization: Creating new hashMap is faster than clearing the existing one
		data.clear();
		
		for (Column col : colList) {
			data.put(col.getColumnName(), col);
		}
		return data;
	}
	
	private static int[] boundedScanByConjunct(Map<String, Column> data, PredicateTreeNode conjunct,
			int[] bounds) throws NotLeafNodeException, InvalidPredicateException {

		/** Get the conjunct's element */
		String colName = conjunct.getColumnName();
		ComparisonOperator op = conjunct.getComparisonOperator();

		int matchNo = 0;

		Column col = data.get(colName);
		int n = bounds.length;
		int[] tmpResult = new int[n], result;
		
		Type type = col.getColumnType();
		
		/** Equality */
		if (op == ComparisonOperator.EQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] == value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] == value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] == value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] == value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				Object value = conjunct.getValue();
				Object[] values = (Object[])col.getDataArrayAsObject();

				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ 
							&& value.equals(values[bounds[i]]))
							tmpResult[matchNo++] = bounds[i];
				}
			}
		}

		/** Greater than or equal */
		else if (op == ComparisonOperator.GEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] >= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] >= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] >= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] >= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[bounds[i]]) >= 0) 
								tmpResult[matchNo++] = bounds[i];
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Less than or equal */
		else if (op == ComparisonOperator.LEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] != Integer.MIN_VALUE
							&& values[bounds[i]] <= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] != Float.MIN_VALUE
							&& values[bounds[i]] <= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] != Double.MIN_VALUE
							&& values[bounds[i]] <= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] != Long.MIN_VALUE
							&& values[bounds[i]] <= value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ 
								&& value.compareTo((Comparable)values[bounds[i]]) <= 0) 
								tmpResult[matchNo++] = bounds[i];
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Less than */
		else if (op == ComparisonOperator.LT) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] != Integer.MIN_VALUE
							&& values[bounds[i]] < value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] != Float.MIN_VALUE
							&& values[bounds[i]] < value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] != Double.MIN_VALUE
							&& values[bounds[i]] < value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] != Long.MIN_VALUE
							&& values[bounds[i]] < value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[bounds[i]]) < 0) 
								tmpResult[matchNo++] = bounds[i];
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Greater than */
		else if (op == ComparisonOperator.GT) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] > value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] > value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] > value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] > value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[bounds[i]]) > 0) 
								tmpResult[matchNo++] = bounds[i];
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Not equal */
		else if (op == ComparisonOperator.NEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Integer.MAX_VALUE && values[bounds[i]] != value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Float.MAX_VALUE && values[bounds[i]] != value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Double.MAX_VALUE && values[bounds[i]] != value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[bounds[i]] != Long.MAX_VALUE && values[bounds[i]] != value) 
						tmpResult[matchNo++] = bounds[i];					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[bounds[i]] != null && values[bounds[i]] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[bounds[i]]) != 0) 
							tmpResult[matchNo++] = bounds[i];
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** OOp! */
		else throw new InvalidPredicateException();

		result =new int[matchNo];
		System.arraycopy(tmpResult, 0, result, 0, matchNo);

		return result;
	}
	
	private static int[] singleBoundedSelect(Map<String, Column> data, String relation, 
			PredicateTreeNode pTreeNode, IndexLayer indexLayer, int[] bounds) 
	throws InvalidPredicateException, SchemaMismatchException, NoSuchTableException {

		if (pTreeNode.isLeaf()) {
			try {
				String colName = pTreeNode.getColumnName();
				ComparisonOperator op = pTreeNode.getComparisonOperator();
				Object value = pTreeNode.getValue();
				
				String[] indexNames = indexLayer.findIndex(relation, colName);
				
				if (indexNames != null && indexNames.length > 0)
					return indexedSelectByConjunct(indexNames, pTreeNode, indexLayer);
				else if (bounds != null)
					return boundedScanByConjunct(data, pTreeNode, bounds);
				else return scanByConjunct(data, pTreeNode);
				
			} catch (NotLeafNodeException e) {
				throw new InvalidPredicateException();
			} catch (NoSuchIndexException e) {
				throw new SchemaMismatchException();
			}
		}
		else {
			try {
				PredicateTreeNode leftChild = pTreeNode.getLeftChild();
				PredicateTreeNode rightChild = pTreeNode.getRightChild();
				LogicalOperator logicOp = pTreeNode.getLogicalOperator();
				
				//AND
				if (logicOp == LogicalOperator.AND) {
					if (rightChild.isLeaf()) {
						int[] result = singleBoundedSelect(data, relation, rightChild, indexLayer, bounds);
						return singleBoundedSelect(data, relation, leftChild, indexLayer, result);
					}
					else {
						int[] result = singleBoundedSelect(data, relation, leftChild, indexLayer, bounds);
						return singleBoundedSelect(data, relation, rightChild, indexLayer, result);
					}
				}
				//OR
				else {
					int[] leftResult = singleBoundedSelect(data, relation, leftChild, indexLayer, bounds);
					int[] rightResult = singleBoundedSelect(data, relation, pTreeNode, indexLayer, bounds);
					
					//ERROR-PRONE: the following merge assumes that the results are ordered.
					int l = leftResult.length, r = rightResult.length;
					int[] result = new int[l + r];
					
					int curL = 0, curR = 0, i = 0;
					
					while (true) {

						if (curL >= l)
							if (curR <= r - 1)
								result[i++] = rightResult[curR++];
							else break;
						else if (curR >= r)
							result[i++] = leftResult[curL++];
						else if (leftResult[curL] < rightResult[curR])
							result[i++] = leftResult[curL++];
						else if (leftResult[curL] > rightResult[curR])
							result[i++] = rightResult[curR++];
						else {
							result[i++] = leftResult[curL++]; 
							curR++;
						}
					}
					int[] finalResult = new int[i];
					System.arraycopy(result, 0, finalResult, 0, i);
					return finalResult;
					
				}
			}
			catch (IsLeafException e) {
				throw new InvalidPredicateException();
			}
		}
	}

	private static int [] boundedSelect(Map<String, Column> data, PredicateTreeNode pTreeNode, int[] bounds)
	throws InvalidPredicateException {
		if (pTreeNode.isLeaf()) {
			try {
				String colName = pTreeNode.getColumnName();
				ComparisonOperator op = pTreeNode.getComparisonOperator();
				Object value = pTreeNode.getValue();
				if (bounds != null)
					return boundedScanByConjunct(data, pTreeNode, bounds);
				else return scanByConjunct(data, pTreeNode);				
			} catch (NotLeafNodeException e) {
				throw new InvalidPredicateException();
			}
		}
		else {
			try {
				PredicateTreeNode leftChild = pTreeNode.getLeftChild();
				PredicateTreeNode rightChild = pTreeNode.getRightChild();
				LogicalOperator logicOp = pTreeNode.getLogicalOperator();
				
				//AND
				if (logicOp == LogicalOperator.AND) {
					if (rightChild.isLeaf()) {
						int[] result = boundedSelect(data, rightChild, bounds);
						return boundedSelect(data, leftChild, result);
					}
					else {
						int[] result = boundedSelect(data, leftChild, bounds);
						return boundedSelect(data, rightChild, result);
					}
				}
				//OR
				else {
					int[] leftResult = boundedSelect(data, leftChild, bounds);
					int[] rightResult = boundedSelect(data, pTreeNode, bounds);
					
					//ERROR-PRONE: the following merge assumes that the results are ordered.
					int l = leftResult.length, r = rightResult.length;
					int[] result = new int[l + r];
					
					int curL = 0, curR = 0, i = 0;
					
					while (true) {

						if (curL >= l)
							if (curR <= r - 1)
								result[i++] = rightResult[curR++];
							else break;
						else if (curR >= r)
							result[i++] = leftResult[curL++];
						else if (leftResult[curL] < rightResult[curR])
							result[i++] = leftResult[curL++];
						else if (leftResult[curL] > rightResult[curR])
							result[i++] = rightResult[curR++];
						else {
							result[i++] = leftResult[curL++]; 
							curR++;
						}
					}
					int[] finalResult = new int[i];
					System.arraycopy(result, 0, finalResult, 0, i);
					return finalResult;
					
				}
			}
			catch (IsLeafException e) {
				throw new InvalidPredicateException();
			}
		}
	}
	
	private static int[] indexedSelectByConjunct(String[] indexNames, PredicateTreeNode predicate, 
			IndexLayer indexLayer) throws NotLeafNodeException, NoSuchIndexException, 
			SchemaMismatchException, NoSuchTableException, InvalidPredicateException {

		/** Get the conjunct's element */
		String colName = predicate.getColumnName();
		ComparisonOperator op = predicate.getComparisonOperator();
		Object value = predicate.getValue();

		/** Look for the appropriate index, if we are processing the single-relation query */	
		try {
			
				int n = indexNames.length;
				String indexName;
				/** 1.1- Equality condition, prefer hash index over tree index */
				if (op == ComparisonOperator.EQ) {					
					for (int i = 0; i < n - 1; i++) {
						indexName = indexNames[i];
						if (!indexLayer.supportsRangeQueries(indexName)) 
							return indexLayer.pointQueryRowIDs(indexName, value);	
					}
					return indexLayer.pointQueryRowIDs(indexNames[n - 1], value); 
				}
				//TODO Might need to estimate the result size here, determine if it's
				//better to utilize index or to scan table

				/** 1.2- Greater or equal */
				else if (op == ComparisonOperator.GEQ) {
					for (int i = 0; i < n; i++) {
						indexName = indexNames[i];							
						if (indexLayer.supportsRangeQueries(indexName)) 
							return indexLayer.rangeQueryRowIDs(indexName, value, null);
					}
				}

				/** 1.3- Less or equal */
				else if (op == ComparisonOperator.LEQ) {
					for (int i = 0; i < n; i++) {
						indexName = indexNames[i];							
						if (indexLayer.supportsRangeQueries(indexName)) 
							return indexLayer.rangeQueryRowIDs(indexName, null, value);
					}
				}

				/** 1.4- Greater */
				else if (op == ComparisonOperator.GT) {
					int[] lowerbounds, bounds;

					for (int i = 0; i < n; i++) {
						indexName = indexNames[i];							
						if (indexLayer.supportsRangeQueries(indexName)) {
							lowerbounds = indexLayer.pointQueryRowIDs(indexName, value);
							bounds = indexLayer.rangeQueryRowIDs(indexName, value, null);

							//TODO might need to sort two arrays, then retrieve the exclusion
							int m = lowerbounds.length;
							if (m > 0) {
								int p = bounds.length;
								int result[] = new int[p - m];
								boolean match = false;
								for (int j = 0; j < p; j++) {
									for (int k = 0; k < m; k++) {
										if (bounds[j] == lowerbounds[k]) {
											match = true;
											break;
										}													
									}
									if (!match) result[j] = bounds[j];
									else match = false;
								}
								//TODO think about the alternatives. This is dangerous !!
								return result;
							}				
							else return bounds;
						}						
					}
				}

				/** 1.5- Less */
				else if (op == ComparisonOperator.LT) {
					int[] upperbounds, bounds;

					for (int i = 0; i < n; i++) {
						indexName = indexNames[i];							
						if (indexLayer.supportsRangeQueries(indexName)) {
							upperbounds = indexLayer.pointQueryRowIDs(indexName, value);
							bounds = indexLayer.rangeQueryRowIDs(indexName, null, value);

							//TODO might need to sort two arrays, then retrieve the exclusion
							int m = upperbounds.length;
							if (m > 0) {
								int p = bounds.length;
								int result[] = new int[p - m];
								boolean match = false;
								for (int j = 0; j < p; j++) {
									for (int k = 0; k < m; k++) {
										if (bounds[j] == upperbounds[k]) {
											match = true;
											break;
										}													
									}
									if (!match) result[j] = bounds[j];
									else match = false;
								}
								//TODO think about the alternatives. This is dangerous !!
								return result;
							}				
							else return bounds;
						}						
					}
				}

				/** 1.6- Not equal*/
				else if (op == ComparisonOperator.NEQ) {
					int[] matches, all;

					for (int i = 0; i < n; i++) {
						indexName = indexNames[i];							
						if (indexLayer.supportsRangeQueries(indexName)) {
							matches = indexLayer.pointQueryRowIDs(indexName, value);
							all = indexLayer.rangeQueryRowIDs(indexName, null, null);

							//TODO might need to sort two arrays, then retrieve the exclusion
							int m = matches.length;
							if (m > 0) {
								int p = all.length;
								int result[] = new int[p - m];
								boolean match = false;
								for (int j = 0; j < p; j++) {
									for (int k = 0; k < m; k++) {
										if (all[j] == matches[k]) {
											match = true;
											break;
										}													
									}
									if (!match) result[j] = all[j];
									else match = false;
								}
								//TODO think about the alternatives. This is dangerous !!
								return result;
							}				
							else return all;
						}						
					}
				}	
		}
		catch (NullPointerException ex) {
			throw new NoSuchIndexException();
		}
		catch (InvalidKeyException ex) {
			throw new InvalidPredicateException();
		} catch (InvalidRangeException e) {
			e.printStackTrace();
			throw new NoSuchIndexException();
		} catch (RangeQueryNotSupportedException e) {
			e.printStackTrace();
			throw new NoSuchIndexException();
		}
		return null;
	}

	private static int[] scanByConjunct(Map<String, Column> data, PredicateTreeNode conjunct)
	throws NotLeafNodeException, InvalidPredicateException {
		
		/** Get the conjunct's element */
		String colName = conjunct.getColumnName();
		ComparisonOperator op = conjunct.getComparisonOperator();

		int matchNo = 0;

		Column col = data.get(colName);
		int n = col.getRowCount();
		int[] tmpResult = new int[n], result;
		
		Type type = col.getColumnType();
		
		/** Equality */
		if (op == ComparisonOperator.EQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] == value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] == value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] == value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] == value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				Object value = conjunct.getValue();
				Object[] values = (Object[])col.getDataArrayAsObject();

				for (int i = 0; i < n; i++) {
					if (values[i] != null && values[i] != MyNull.NULLOBJ 
							&& value.equals(values[i]))
							tmpResult[matchNo++] = i;
				}
			}
		}

		/** Greater than or equal */
		else if (op == ComparisonOperator.GEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] >= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] >= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] >= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] >= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[i] != null && values[i] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[i]) >= 0) 
								tmpResult[matchNo++] = i;
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Less than or equal */
		else if (op == ComparisonOperator.LEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] != Integer.MIN_VALUE
							&& values[i] <= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] != Float.MIN_VALUE
							&& values[i] <= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] != Double.MIN_VALUE
							&& values[i] <= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] != Long.MIN_VALUE
							&& values[i] <= value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[i] != null && values[i] != MyNull.NULLOBJ 
								&& value.compareTo((Comparable)values[i]) <= 0) 
								tmpResult[matchNo++] = i;
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Less than */
		else if (op == ComparisonOperator.LT) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] != Integer.MIN_VALUE
							&& values[i] < value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] != Float.MIN_VALUE
							&& values[i] < value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] != Double.MIN_VALUE
							&& values[i] < value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] != Long.MIN_VALUE
							&& values[i] < value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[i] != null && values[i] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[i]) < 0) 
								tmpResult[matchNo++] = i;
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Greater than */
		else if (op == ComparisonOperator.GT) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] > value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] > value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] > value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] > value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[i] != null && values[i] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[i]) > 0) 
								tmpResult[matchNo++] = i;
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** Not equal */
		else if (op == ComparisonOperator.NEQ) {
			if (type == Types.getIntegerType()) {

				//Avoid un-boxing, which can invoke Garbage Collector later
				int value = ((Integer)conjunct.getValue()).intValue();
				int[] values = (int[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Integer.MAX_VALUE && values[i] != value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getFloatType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				float value = ((Float)conjunct.getValue()).floatValue();
				float[] values = (float[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Float.MAX_VALUE && values[i] != value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getDoubleType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				double value = ((Double)conjunct.getValue()).doubleValue();
				double[] values = (double[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Double.MAX_VALUE && values[i] != value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else if (type == Types.getLongType()) {
				//Avoid un-boxing, which can invoke Garbage Collector later
				long value = ((Long)conjunct.getValue()).longValue();
				long[] values = (long[])col.getDataArrayAsObject();
				for (int i = 0; i < n; i++) {
					if (values[i] != Long.MAX_VALUE && values[i] != value) 
						tmpResult[matchNo++] = i;					
				}
			}
			else {
				//ERROR-PRONE
				try {
					Comparable value = (Comparable)conjunct.getValue();
					Object[] values = (Object[])col.getDataArrayAsObject();
	
					for (int i = 0; i < n; i++) {
						if (values[i] != null && values[i] != MyNull.NULLOBJ
								&& value.compareTo((Comparable)values[i]) != 0) 
							tmpResult[matchNo++] = i;
					}
				}
				catch (ClassCastException ex) {
					throw new InvalidPredicateException();
				}
			}
		}

		/** OOp! */
		else throw new InvalidPredicateException();

		result =new int[matchNo];
		System.arraycopy(tmpResult, 0, result, 0, matchNo);

		return result;
	}
}