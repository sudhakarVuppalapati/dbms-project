package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import operator.Operator;
import metadata.Type;
import metadata.Types;
import myDB.physicaloperators.CrossProductOperator;
import myDB.physicaloperators.JoinOperator;
import myDB.physicaloperators.ProjectionOperator;
import myDB.physicaloperators.SelectionOperator;
import myDB.physicaloperators.TempRow;
import relationalalgebra.CrossProduct;
import relationalalgebra.Input;
import relationalalgebra.Join;
import relationalalgebra.Projection;
import relationalalgebra.RelationalAlgebraExpression;
import relationalalgebra.Selection;
import systeminterface.Column;
import systeminterface.IndexLayer;
import systeminterface.QueryLayer;
import systeminterface.Row;
import systeminterface.StorageLayer;
import systeminterface.Table;
import util.RelationalOperatorType;
import exceptions.ColumnAlreadyExistsException;
import exceptions.InvalidKeyException;
import exceptions.InvalidPredicateException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchIndexException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * Query Layer. Implement this skeleton. You have to use the constructor shown
 * here.
 */
public class MyQueryLayer implements QueryLayer {

	private final StorageLayer storageLayer;

	private final IndexLayer indexLayer;
	
	/**
	 * 
	 * Constructor, can only know about lower layers. Please do not modify this
	 * constructor
	 * 
	 * @param storageLayer
	 *            A reference to the underlying storage layer
	 * @param indexLayer
	 *            A reference to the underlying index layer
	 * 
	 */
	public MyQueryLayer(StorageLayer storageLayer, IndexLayer indexLayer) {

		this.storageLayer = storageLayer;
		this.indexLayer = indexLayer;
	}

	@Override
	public void addColumn(String tableName, String columnName, Type columnType)
	throws NoSuchTableException, ColumnAlreadyExistsException {

		Table t = storageLayer.getTableByName(tableName);
		t.addColumn(columnName, columnType);
	}

	@Override
	public void createTable(String tableName, Map<String, Type> schema)
	throws TableAlreadyExistsException {

		storageLayer.createTable(tableName, schema);
	}

	@Override
	public void deleteRow(String tableName, Row row)
	throws NoSuchTableException, NoSuchRowException,
	SchemaMismatchException {

		Table t = storageLayer.getTableByName(tableName);

		/**
		 * The deleting is costly, think about it. One possible alternative:
		 * Increase coupling between MyQueryLayer and MyTable, create methods
		 * like deleteIndexesByRow(Row) in MyIndexLayer
		 * 
		 * Other possible solution: Re-design :)
		 * 
		 * The best compromising solution: Postpone the synchronization between
		 * indexes and base tables. Everytime we delete a row, we create a
		 * thread to look and update the corresponding data entries. In the
		 * meanwhile, when we search for data of a search key, we need to ensure
		 * that the underlying data is consistent.
		 * 
		 * I'm currently using approach 3. In this approach, I just delete row
		 * from the table and then do nothing.
		 * 
		 * @author tuanta
		 */

		/** TENTATIVE - $BEGIN */
		String[] indexes, colNames = row.getColumnNames();

		int[] tupleIDs = null;
		boolean checked = false;

		try {
			//Pass 1: Scan indexes to retrieve list of matching tuple IDs
			for (String col : colNames) {
				indexes = indexLayer.findIndex(tableName, col);
				if (indexes != null) {
					for (String index : indexes) {
						if (!checked) {
							tupleIDs = indexLayer.pointQueryRowIDs(index, row.getColumnValue(col));
							if (tupleIDs != null && tupleIDs.length > 0){
								checked = true;
								break;
							}
						}
					}
					if (checked) break;
					else throw new NoSuchRowException();
				}				
			}

			//Pass 2: Delete in indexes and in table
			if (!checked) {	//There is no index
				tupleIDs = ((MyTable) t).getRowID(row);
				if (tupleIDs.length == 0)
					throw new NoSuchRowException();
			}
				for (int m = 0, p = tupleIDs.length; m < p; m++) {
					for (int i = 0, n = colNames.length; i < n; i++) {
						indexes = indexLayer.findIndex(tableName, colNames[i]);
						if (indexes != null)
							for (int j = 0, q = indexes.length; j < q; j++) {
								indexLayer.deleteFromIndex(indexes[j], row
										.getColumnValue(colNames[i]), tupleIDs[m]);
							}
					}
					t.deleteRow(tupleIDs[m]);
				}
			
		}
		catch (InvalidKeyException ex) {
			throw new SchemaMismatchException();
		}
		catch (NoSuchIndexException w) {
			throw new SchemaMismatchException();
		}
		catch (NoSuchColumnException e) {
			throw new SchemaMismatchException();
		}
	}

	// Need review of try catch
	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		Table t = storageLayer.getTableByName(tableName);

		Operator<Column> cols = (Operator<Column>) t.getAllColumns();
		Column col;
		String[] indexNames;

		try {
			while ((col = cols.next()) != null) {
				indexNames = indexLayer.findIndex(tableName, col
						.getColumnName());
				if (indexNames != null)
					for (int i = 0, n = indexNames.length; i < n; i++)
						indexLayer.dropIndex(indexNames[i]);
			}
		} catch (SchemaMismatchException e) {
			e.printStackTrace();
			// make nonsense to throw NoSuchTableException here
			throw new NoSuchTableException();
		} catch (NoSuchIndexException e) {
			e.printStackTrace();
			// make nonsense to throw NoSuchTableException here
			throw new NoSuchTableException();
		}

		storageLayer.deleteTable(tableName);

		// TODO Some code in query layers are written here

	}

	// Need review of try catch
	@Override
	public void dropColumn(String tableName, String columnName)
	throws NoSuchTableException, NoSuchColumnException {

		String[] indexNames;
		Table t = storageLayer.getTableByName(tableName);

		try {
			indexNames = indexLayer.findIndex(tableName, columnName);
		} catch (SchemaMismatchException e1) {
			e1.printStackTrace();
			throw new NoSuchColumnException();
		}

		try {
			if (indexNames != null)
				for (int i = 0, n = indexNames.length; i < n; i++)
					indexLayer.dropIndex(indexNames[i]);
		} catch (NoSuchIndexException e) {
			e.printStackTrace();
			throw new NoSuchColumnException();
		}

		t.dropColumnByName(columnName);
	}

	@Override
	public void insertRow(String tableName, Row row)
	throws NoSuchTableException, SchemaMismatchException {

		Table t = storageLayer.getTableByName(tableName);

		int size = ((MyTable) t).getAllRowCount();

		try {
			String[] colNames = row.getColumnNames();
			String[] indexes;

			try {
				for (int i = 0, n = colNames.length; i < n; i++) {
					indexes = indexLayer.findIndex(tableName, colNames[i]);
					if (indexes != null)
						for (int j = 0, m = indexes.length; j < m; j++) {
							indexLayer.insertIntoIndex(indexes[j], row
									.getColumnValue(colNames[i]), size);
						}
				}
			} catch (NoSuchIndexException e) {
				e.printStackTrace();
				throw new SchemaMismatchException();
			} catch (InvalidKeyException e) {
				e.printStackTrace();
				throw new SchemaMismatchException();
			}

		} catch (NoSuchColumnException e) {
			e.printStackTrace();
			throw new SchemaMismatchException();
		}

		t.addRow(row);
	}

	@Override
	public void renameColumn(String tableName, String oldColumnName,
			String newColumnName) throws NoSuchTableException,
			ColumnAlreadyExistsException, NoSuchColumnException {

		Table t = storageLayer.getTableByName(tableName);
		t.renameColumn(oldColumnName, newColumnName);

	}

	@Override
	public void renameTable(String oldName, String newName)
	throws TableAlreadyExistsException, NoSuchTableException {
		storageLayer.renameTable(oldName, newName);
	}

	// Pending
	@Override
	public void updateRow(String tableName, Row oldRow, Row newRow)
	throws NoSuchRowException, NoSuchTableException,
	SchemaMismatchException {

		Table t;
		t = storageLayer.getTableByName(tableName);

		/** TENTATIVE - $BEGIN */

		String[] indexes, colNames = oldRow.getColumnNames();
		int[] tupleIDs = null;

		boolean checked = false;

		try {
			//Pass 1: Scan indexes to retrieve list of matching tuple IDs
			for (String col : colNames) {
				indexes = indexLayer.findIndex(tableName, col);
				if (indexes != null) {
					for (String index : indexes) {
						if (!checked) {
							tupleIDs = indexLayer.pointQueryRowIDs(index, oldRow.getColumnValue(col));
							if (tupleIDs != null && tupleIDs.length > 0){
								checked = true;
								break;
							}
						}
					}
					if (checked) break;
					else throw new NoSuchRowException();
				}				
			}

			//Pass 2: Delete in indexes and in table
			if (!checked) {	//There is no index
				tupleIDs = ((MyTable) t).getRowID(oldRow);
				if (tupleIDs.length == 0)
					throw new NoSuchRowException();
			}
			for (int m = 0, p = tupleIDs.length; m < p; m++) {
				for (int i = 0, n = colNames.length; i < n; i++) {
					indexes = indexLayer.findIndex(tableName, colNames[i]);
					if (indexes != null)
						for (int j = 0, q = indexes.length; j < q; j++) {
							indexLayer.deleteFromIndex(indexes[j], oldRow
									.getColumnValue(colNames[i]), tupleIDs[m]);
							indexLayer.insertIntoIndex(indexes[j], newRow
									.getColumnValue(colNames[i]), tupleIDs[m]);

						}
				}
				t.updateRow(tupleIDs[m], newRow);
			}

		}
		catch (InvalidKeyException ex) {
			throw new SchemaMismatchException();
		}
		catch (NoSuchIndexException w) {
			throw new SchemaMismatchException();
		}
		catch (NoSuchColumnException e) {
			throw new SchemaMismatchException();
		}

		/** TENTATIVE - $END */
	}

	@Override
	public Operator<? extends Row> query(RelationalAlgebraExpression query)
	throws NoSuchTableException, SchemaMismatchException,
	NoSuchColumnException, InvalidPredicateException {

		Optimizer queryOptimizer = new Optimizer(storageLayer);
		Map<String, Column> mapResult = adaptedQuery(queryOptimizer.optimize(query));
		
		// TODO implement optimization here. This is naive and incomplete
		//Map<String, Column> mapResult = adaptedQuery(query);

		if (mapResult == null)
			throw new SchemaMismatchException(); // check this verification - it
		// doesn't make sense

		// Retrieve the first column, counting the number of row;
		Collection<Column> cols = mapResult.values();

		int n = cols.iterator().next().getRowCount();
		List<Row> rowResult = new ArrayList<Row>();
		// Object[] content;
		Map<String, Object> content;
		/* ColumnInfo colInfo; */
		Map<String, Type> schema = new HashMap<String, Type>();

		// Create Column Info
		for (Column col : cols) {
			schema.put(col.getColumnName(), col.getColumnType());
		}

		Type type;
		for (int i = 0; i < n; i++) {
			int j = 0;
			/* content = new Object[mapResult.size()]; */
			content = new HashMap<String, Object>();

			for (Column col : cols) {
				type = col.getColumnType();

				if (type == Types.getIntegerType()) {
					int[] data = (int[]) col.getDataArrayAsObject();
					// boxing
					if (data[i] != Integer.MAX_VALUE)
						/* content[j++] = new Integer(data[i]); */
						content.put(col.getColumnName(), new Integer(data[i]));
				}

				else if (type == Types.getFloatType()) {
					float[] data = (float[]) col.getDataArrayAsObject();
					// boxing
					if (data[i] != Float.MAX_VALUE)
						/* content[j++] = new Float(data[i]); */
						content.put(col.getColumnName(), new Float(data[i]));
				}

				else if (type == Types.getDoubleType()) {
					double[] data = (double[]) col.getDataArrayAsObject();
					// boxing
					if (data[i] != Double.MAX_VALUE)
						/* content[j++] = new Double(data[i]); */
						content.put(col.getColumnName(), new Double(data[i]));
				} else if (type == Types.getLongType()) {
					long[] data = (long[]) col.getDataArrayAsObject();
					// boxing
					if (data[i] != Long.MAX_VALUE)
						/* content[j++] = new Long(data[i]); */
						content.put(col.getColumnName(), new Long(data[i]));
				} else {
					Object[] data = (Object[]) col.getDataArrayAsObject();
					// reference, not copy
					if (data[i] != MyNull.NULLOBJ)
						/* content[j++] = data[i]; */
						content.put(col.getColumnName(), data[i]);
				}
			}
			if (content.size() > 0)
				rowResult.add(new TempRow(schema, content));
		}
		return new MyOperator(rowResult);
	}

	private Map<String, Column> adaptedQuery(RelationalAlgebraExpression query)
	throws NoSuchTableException, SchemaMismatchException,
	NoSuchColumnException, InvalidPredicateException {

		RelationalOperatorType qType = query.getType();
		Map<String, Column> processingResult = new HashMap<String, Column>();

		if (qType == RelationalOperatorType.INPUT) {
			try {
				Operator<Column> tableAsCols = (Operator<Column>) storageLayer.getTableByName(((Input)query).getRelationName()).getAllColumns();

				tableAsCols.open();
				Column c;
				Type t;
				while ((c = tableAsCols.next()) != null) {

					t = c.getColumnType();
					int colSize = c.getRowCount();
					Object oldArray = c.getDataArrayAsObject();

					if (t == Types.getIntegerType()) {
						int[] newData = new int[colSize];
						System.arraycopy(oldArray, 0, newData, 0, colSize);
						c = new MyIntColumn(c.getColumnName(), newData);
					} else if (t == Types.getLongType()) {
						long[] newData = new long[colSize];
						System.arraycopy(oldArray, 0, newData, 0, colSize);
						c = new MyLongColumn(c.getColumnName(), newData);
					} else if (t == Types.getFloatType()) {
						float[] newData = new float[colSize];
						System.arraycopy(oldArray, 0, newData, 0, colSize);
						c = new MyFloatColumn(c.getColumnName(), newData);
					} else if (t == Types.getDoubleType()) {
						double[] newData = new double[colSize];
						System.arraycopy(oldArray, 0, newData, 0, colSize);
						c = new MyDoubleColumn(c.getColumnName(), newData);
					} else {
						Object[] newData = new Object[colSize];
						System.arraycopy(oldArray, 0, newData, 0, colSize);
						c = new MyObjectColumn(c.getColumnName(), c
								.getColumnType(), newData);
					}
					processingResult.put(c.getColumnName(), c);
				}

				tableAsCols.close();
				return processingResult;
			} catch (NoSuchTableException ex) {
				throw new SchemaMismatchException();
			}
		}

		else if (qType == RelationalOperatorType.PROJECTION) {
			Projection curNode = (Projection) query;
			return ProjectionOperator.projectWithDuplicates(
					adaptedQuery(curNode.getInput()), curNode
					.getProjectionAttributes());

		} else if (qType == RelationalOperatorType.SELECTION) {

			Selection curNode = (Selection) query;
			RelationalAlgebraExpression input = curNode.getInput();

			String relation = (input.getType() == RelationalOperatorType.INPUT) ? ((Input) input)
					.getRelationName()
					: null;

					return SelectionOperator.select(adaptedQuery(input), curNode
							.getPredicate(), relation, indexLayer);

		} else if (qType == RelationalOperatorType.JOIN) {
			Join join = (Join) query;
			
			RelationalAlgebraExpression rae1=join.getRightInput();
			
			Index index = null;
			
			//if the right table is an original/physical table , not a table corresponding to a temporary result
			if(rae1.getType() == RelationalOperatorType.INPUT ){
				String[] indexNames= this.indexLayer.findIndex(((Input)rae1).getRelationName(), join.getRightJoinAttribute());
				if( indexNames !=null && indexNames.length !=0){
					index = ((MyIndexLayer)indexLayer).getIndexByName(indexNames[0]); //get the first index in the array
					//probably here I should also check that the index  doesn't support rangeQuery which means it's a hash index 
					
					//System.out.println("Using index join on " + indexNames[0]);
					
					return JoinOperator.joinIndexNested(adaptedQuery(join.getLeftInput()),adaptedQuery(join.getRightInput()),
							index, join.getLeftJoinAttribute(), join.getRightJoinAttribute());
				}
				
				
			}
			
			RelationalAlgebraExpression rae2= join.getLeftInput();
			
			if(rae2.getType() == RelationalOperatorType.INPUT ){
				String[] indexNames= this.indexLayer.findIndex(((Input)rae2).getRelationName(), join.getLeftJoinAttribute());
				if( indexNames !=null && indexNames.length !=0){
					index = ((MyIndexLayer)indexLayer).getIndexByName(indexNames[0]); //get the first index in the array
					//probably here I should also check that the index  doesn't support rangeQuery which means it's a hash index 
					
					return JoinOperator.joinIndexNested(adaptedQuery(join.getRightInput()),adaptedQuery(join.getLeftInput()),
																					index, join.getRightJoinAttribute(),  join.getLeftJoinAttribute());
				}
			
			}
			
			//System.out.println("Using simple join");
			
			return JoinOperator.joinSimple(adaptedQuery(join.getLeftInput()),
					adaptedQuery(join.getRightInput()), join.getLeftJoinAttribute(), join.getRightJoinAttribute());
			
		} else { // for cross product
			CrossProduct curNode = (CrossProduct) query;
			RelationalAlgebraExpression leftInput = curNode.getLeftInput();
			RelationalAlgebraExpression rightInput = curNode.getRightInput();

			return CrossProductOperator.product(adaptedQuery(leftInput),
					adaptedQuery(rightInput));
		}

	}

	@Override
	public String explain(RelationalAlgebraExpression root)
	throws NoSuchTableException, SchemaMismatchException,
	NoSuchColumnException, InvalidPredicateException {
		
		StringBuilder str = new StringBuilder();
		explain1(root, str);
		return str.toString();
	}

	private void explain1(RelationalAlgebraExpression root, StringBuilder str) {
		
		if(root.getType() == RelationalOperatorType.JOIN){

			str.append("( ");
			explain1(((Join)root).getLeftInput(), str);
			str.append(" )");

			//str.appendln("\u27D7");
			str.append(" JOIN ");

			str.append("( ");
			explain1(((Join)root).getRightInput(), str);
			str.append(" )");
		}

		else if(root.getType() == RelationalOperatorType.CROSS_PRODUCT){

			str.append("( ");
			explain1(((CrossProduct)root).getLeftInput(), str);
			str.append(" )");

			str.append("X");

			str.append("( ");
			explain1(((CrossProduct)root).getRightInput(), str);
			str.append(" )");
		}

		else if(root.getType() == RelationalOperatorType.PROJECTION){

			/*str.appendln("\u03A0");*/
			str.append(" PRJ ");
			str.append("( ");
			explain1(((Projection)root).getInput(), str);
			str.append(" )");

		}

		else if(root.getType() == RelationalOperatorType.SELECTION){

			/*str.appendln("\u03C3");*/
			str.append(" SELECT ");
			str.append("( ");
			explain1(((Selection)root).getInput(), str);
			str.append(" )");

		}

		else{
			str.append(((Input)root).getRelationName());
		}
	}
	
	public RelationalAlgebraExpression analyse(RelationalAlgebraExpression query) 
	throws InvalidPredicateException, NoSuchColumnException {
		Optimizer queryOptimizer = new Optimizer(storageLayer);
		return queryOptimizer.optimize(query);
	}
}
