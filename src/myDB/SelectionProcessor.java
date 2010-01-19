package myDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import metadata.Type;

import operator.Operator;
import relationalalgebra.CrossProduct;
import relationalalgebra.Input;
import relationalalgebra.Join;
import relationalalgebra.Projection;
import relationalalgebra.RelationalAlgebraExpression;
import relationalalgebra.Selection;
import systeminterface.IndexLayer;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.StorageLayer;
import util.ComparisonOperator;
import util.LogicalOperator;
import util.RelationalOperatorType;
import exceptions.InvalidKeyException;
import exceptions.InvalidPredicateException;
import exceptions.InvalidRangeException;
import exceptions.IsLeafException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchTableException;
import exceptions.NotLeafNodeException;
import exceptions.RangeQueryNotSupportedException;
import exceptions.SchemaMismatchException;

public class SelectionProcessor {

	public static String explain(Selection query, IndexLayer iLayer, StorageLayer sLayer)
	throws SchemaMismatchException {
		// TODO Auto-generated method stub
		return null;
	}

	public static Operator<? extends Row> query(Selection query, IndexLayer iLayer, 
			StorageLayer sLayer) throws SchemaMismatchException, NoSuchTableException, 
			InvalidPredicateException {
		
		RelationalAlgebraExpression input = query.getInput();
		PredicateTreeNode predicate = query.getPredicate();
		
		return query(predicate, input, iLayer, sLayer);
	}
	
	private static Operator<? extends Row> query(PredicateTreeNode predicate, 
			RelationalAlgebraExpression input, IndexLayer indexLayer, 
			StorageLayer storageLayer) 
			throws NoSuchTableException, SchemaMismatchException, 
			InvalidPredicateException {
		if (predicate.isLeaf())
			return queryConjunct(predicate, input, indexLayer, storageLayer);
		else {
			try {
				Operator<? extends Row> leftRows = null, rightRows = null;
			
				PredicateTreeNode leftNode = predicate.getLeftChild();
				PredicateTreeNode rightNode = predicate.getRightChild();
			
				if (leftNode != null)
					leftRows = query(leftNode, input, indexLayer, storageLayer);
				if (rightNode != null)
					rightRows = query(rightNode, input, indexLayer, storageLayer);
			
				LogicalOperator logicOp = predicate.getLogicalOperator();
				
				Row r; 
				if (logicOp == LogicalOperator.OR) {
					Map result = new HashMap();
					leftRows.open(); 
					rightRows.open();
					while ((r = leftRows.next()) != null)
						result.put(r, null);
					while ((r = rightRows.next()) != null)
						result.put(r, null);
					leftRows.close();
					rightRows.close();
					return new MyOperator<Row>(result.keySet());
				}
				else {
					Row r1;
					List result = new ArrayList();
					leftRows.open();
					rightRows.open();
					while ((r = leftRows.next()) != null)
						while ((r1 = rightRows.next()) != null)
							if (r == r1) result.add(r);
					leftRows.close();
					rightRows.close();
					return new MyOperator<Row>(result);
				}
			}
			catch (IsLeafException e) {
				throw new InvalidPredicateException();
			}
		}
		
	}

	/**
	 * Query a conjunct, which has the form column op value
	 * @param node
	 * @param input
	 * @param indexLayer
	 * @param storageLayer
	 * @return
	 * @throws NotLeafNodeException
	 * @throws NoSuchTableException
	 * @throws SchemaMismatchException
	 */
	private static Operator<? extends Row> queryConjunct(PredicateTreeNode node, 
			RelationalAlgebraExpression input, IndexLayer indexLayer, StorageLayer storageLayer)
			throws NoSuchTableException, SchemaMismatchException, 
			InvalidPredicateException {
					
		/** First, get the conjunct */
		String colName;
		ComparisonOperator op;
		Object value;
		
		try {
			colName = node.getColumnName();
			op = node.getComparisonOperator();
			value = node.getValue();
		}
		catch (NotLeafNodeException e) {
			throw new InvalidPredicateException();
		}
		
		/** Second, try to get the Row-liked data, depending on the input type */
		RelationalOperatorType type = input.getType();
		
		/** 1- Handle single-relation query. We only need index in this case */
		if (type == RelationalOperatorType.INPUT) {
			String relation = ((Input)input).getRelationName();
			/** Look for the appropriate index, if we are processing the single-relation query */	
			String[] indexNames = indexLayer.findIndex(relation, colName);
			try {
				if (indexNames != null) {
					int n = indexNames.length;
					String indexName;
					/** 1.1- Equality condition, prefer hash index over tree index */
					if (op == ComparisonOperator.EQ) {					
						for (int i = 0; i < n - 1; i++) {
							indexName = indexNames[i];
							if (!indexLayer.supportsRangeQueries(indexName)) 
								return indexLayer.pointQuery(indexName, value);	
						}
						return indexLayer.pointQuery(indexNames[n - 1], value); 
					}
					//TODO Might need to estimate the result size here, determine if it's
					//better to utilize index or to scan table
					
					/** 1.2- Greater or equal */
					else if (op == ComparisonOperator.GEQ) {
						for (int i = 0; i < n; i++) {
							indexName = indexNames[i];							
							if (indexLayer.supportsRangeQueries(indexName)) 
								return indexLayer.rangeQuery(indexName, value, null);
						}
					}
					
					/** 1.3- Less or equal */
					else if (op == ComparisonOperator.LEQ) {
						for (int i = 0; i < n; i++) {
							indexName = indexNames[i];							
							if (indexLayer.supportsRangeQueries(indexName)) 
								return indexLayer.rangeQuery(indexName, null, value);
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
									return ((MyTable)storageLayer.getTableByName(relation)).getRows(result);
								}				
								else return ((MyTable)storageLayer.getTableByName(relation)).getRows(bounds);
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
									return ((MyTable)storageLayer.getTableByName(relation)).getRows(result);
								}				
								else return ((MyTable)storageLayer.getTableByName(relation)).getRows(bounds);
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
									return ((MyTable)storageLayer.getTableByName(relation)).getRows(result);
								}				
								else return ((MyTable)storageLayer.getTableByName(relation)).getRows(all);
							}						
						}
					}					
				}
				/* If no index is available, do scanning */
				return storageLayer.getTableByName(relation).getRows(node);
				
			} catch (NoSuchIndexException nie) {
				throw new InvalidPredicateException();
			} catch (InvalidKeyException e) {
				throw new InvalidPredicateException();
			} catch (InvalidRangeException e) {
				throw new InvalidPredicateException();
			} catch (RangeQueryNotSupportedException e) {
				//TODO review needed: If we find the index and it supports range, but
				//then when we try to perform range query, we get an error. What could 
				//be the scenario ? Which exception should be thrown then ?
				throw new SchemaMismatchException();
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
				throw new SchemaMismatchException();
			}
		}
		
		//pipeline the result bottom up
		else  {
			Operator<? extends Row> rowOp;
			
			/** 2- Handle part-of-relation query */
			if (type == RelationalOperatorType.PROJECTION) {
				rowOp = ProjectionProcessor.query((Projection)input, indexLayer, storageLayer);
			}
			/** 3- Handle cross-product selection query */
			else if (type == RelationalOperatorType.CROSS_PRODUCT) {
				rowOp = CrossProductProcessor.query((CrossProduct)input, indexLayer, storageLayer);
			}
			
			/** 4- Handle Join selection query */
			else if (type == RelationalOperatorType.JOIN) {
				rowOp = JoinProcessor.query((Join)input, indexLayer, storageLayer);
			}
			
			/**5- Handle cascaded selection query */
			else if (type == RelationalOperatorType.SELECTION) {
				rowOp = query((Selection)input, indexLayer, storageLayer);
			}
			
			/** 6- OOp ! */
			else throw new SchemaMismatchException();
			
			//If no data found, return empty operator
			rowOp.open();
			
			Row r;
			List<Row> filteredRows = new ArrayList<Row>();
			
			try {
				while ((r = rowOp.next()) != null) {
					Object colValue = r.getColumnValue(colName);
					Type colType = r.getColumnType(colName);
					if (MyColumn.check(colValue, value, colType, op)) {
						filteredRows.add(r);
					}					
				}
				return new MyOperator<Row>(filteredRows);
			} catch (NoSuchColumnException e) {
				throw new SchemaMismatchException();
			}
		}			
	}
}