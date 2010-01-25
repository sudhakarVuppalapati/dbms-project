package myDB;

import java.util.ArrayList;
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
		
		// TODO Some code in query layers are written here
		
	}

	@Override
	public void createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException {
		
		storageLayer.createTable(tableName, schema);
		
		// TODO Some code in query layers are written here
	}
	

	@Override
	public void deleteRow(String tableName, Row row)
			throws NoSuchTableException, NoSuchRowException,
			SchemaMismatchException {
		
		Table t = storageLayer.getTableByName(tableName);
		
		/**
		 * The deleting is costly, think about it.
		 * One possible alternative: Increase coupling between MyQueryLayer and 
		 * MyTable, create methods like deleteIndexesByRow(Row) in MyIndexLayer
		 * 
		 * Other possible solution: Re-design :)
		 * 
		 * The best compromising solution: Postpone the synchronization between
		 * indexes and base tables. Everytime we delete a row, we create a thread 
		 * to look and update the corresponding data entries. In the meanwhile,
		 * when we search for data of a search key, we need to ensure that the
		 * underlying data is consistent.
		 * 
		 * I'm currently using approach 3. In this approach, I just delete row
		 * from the table and then do nothing.
		 * @author tuanta
		 */
		
		/** TENTATIVE - $BEGIN */
		/**
		 * The following code is too costly. Think about it
		 */
		String[] indexes, colNames = row.getColumnNames();
		int[] k = ((MyTable)t).getRowID(row);
				
		try {
			for (int m = 0, p = k.length; m < p; m++) {
				for (int i = 0, n = colNames.length ; i < n; i++) {
					indexes = indexLayer.findIndex(tableName, colNames[i]);
					if (indexes != null)
						for (int j = 0, q = indexes.length; j < q; j++) {
							indexLayer.deleteFromIndex(indexes[j], row.getColumnValue(colNames[i]), k[m]);
						}
				}	
			}			
			
		} catch (NoSuchIndexException e) {	
			e.printStackTrace();
			throw new SchemaMismatchException();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
			throw new SchemaMismatchException();
		} catch (NoSuchColumnException e) {			
			e.printStackTrace();
			throw new SchemaMismatchException();
		}
		
		t.deleteRow(row);
		
		// TODO Some code in query layers are written here

	}

	//Need review of try catch
	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		Table t = storageLayer.getTableByName(tableName);
		
		Operator<Column> cols = (Operator<Column>) t.getAllColumns();
		Column col;
		String[] indexNames;
		
		try {
			while ((col = cols.next()) != null) {
				indexNames = indexLayer.findIndex(tableName, col.getColumnName());
				if (indexNames != null)
					for (int i = 0, n = indexNames.length; i < n; i++)
						indexLayer.dropIndex(indexNames[i]);				
				}
		} catch (SchemaMismatchException e) {
			e.printStackTrace();		
			//make nonsense to throw NoSuchTableException here
			throw new NoSuchTableException();
		} catch (NoSuchIndexException e) {
			e.printStackTrace();
			//make nonsense to throw NoSuchTableException here
			throw new NoSuchTableException();
		}
		
		storageLayer.deleteTable(tableName);
		
		// TODO Some code in query layers are written here

	}

	//Need review of try catch
	@Override
	public void dropColumn(String tableName, String columnName)
			throws NoSuchTableException, NoSuchColumnException {
		
		String[] indexNames;
		
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
		
		Table t = storageLayer.getTableByName(tableName);
		t.dropColumnByName(columnName);
		// TODO Some code in query layers are written here
	}

	@Override
	public void insertRow(String tableName, Row row)
			throws NoSuchTableException, SchemaMismatchException {
				
		Table t = storageLayer.getTableByName(tableName);
		
		int size = ((MyTable)t).getAllRowCount();
		
		try {
			String[] colNames = row.getColumnNames();
			String[] indexes;
			
			//TODO Might need a better solution
			
			try {
				for (int i = 0, n = colNames.length; i < n; i++) {
					indexes = indexLayer.findIndex(tableName, colNames[i]);
					if (indexes != null)
						for (int j = 0, m = indexes.length; j < m; j++) {
							indexLayer.insertIntoIndex(indexes[j], row.getColumnValue(colNames[i]), size);
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
		
		// TODO Some code in query layers are written here
	}

	@Override
	public void renameColumn(String tableName, String oldColumnName,
			String newColumnName) throws NoSuchTableException,
			ColumnAlreadyExistsException, NoSuchColumnException {
		
		Table t = storageLayer.getTableByName(tableName);	
		t.renameColumn(oldColumnName, newColumnName);
		// TODO Some code in query layers are written here

	}

	@Override
	public void renameTable(String oldName, String newName)
			throws TableAlreadyExistsException, NoSuchTableException {
		storageLayer.renameTable(oldName, newName);
		
		// TODO Some code in query layers are written here
	}

	//Pending
	@Override
	public void updateRow(String tableName, Row oldRow, Row newRow)
			throws NoSuchRowException, NoSuchTableException, 
			SchemaMismatchException {
		
		Table t;			
		t = storageLayer.getTableByName(tableName);
		
		/** TENTATIVE - $BEGIN */
		/**
		 * The following code is too costly. Think about it
		 */
		String[] indexes, colNames = oldRow.getColumnNames();
		int[] k = ((MyTable)t).getRowID(oldRow);
				
		try {
			for (int m = 0, p = k.length; m < p; m++) {
				for (int i = 0, n = colNames.length ; i < n; i++) {
					indexes = indexLayer.findIndex(tableName, colNames[i]);
					if (indexes != null)
						for (int j = 0, q = indexes.length; j < q; j++) {
							indexLayer.deleteFromIndex(indexes[j], oldRow.getColumnValue(colNames[i]), k[m]);
							indexLayer.insertIntoIndex(indexes[j], newRow.getColumnValue(colNames[i]), k[m]);
						}
				}	
			}			
			
		} catch (NoSuchIndexException e) {	
			e.printStackTrace();
			throw new SchemaMismatchException();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
			throw new SchemaMismatchException();
		} catch (NoSuchColumnException e) {			
			e.printStackTrace();
			throw new SchemaMismatchException();
		}		
		/** TENTATIVE - $END */

		t.updateRow(oldRow, newRow);
		
		// TODO Some code in query layers are written here
	}
	
	@Override
	public Operator<? extends Row> query(RelationalAlgebraExpression query) 
	throws NoSuchTableException, SchemaMismatchException, NoSuchColumnException, 
	InvalidPredicateException {
		
		//TODO implement optimization here. This is naive and incomplete
		Map<String, Column> mapResult = (Map<String, Column>) adaptedQuery(query);
		
		if (mapResult == null) throw new SchemaMismatchException();
		
		//Retrieve the first column, counting the number of row;
		int n = mapResult.values().iterator().next().getRowCount();
		List<Row> rowResult = new ArrayList<Row>();
		//Object[] content;
		Map<String, Object> content;
		/*ColumnInfo colInfo;*/
		Map<String, Type> schema = new HashMap<String, Type>();
		
		//Create Column Info
		for (Column col : mapResult.values()) {
			schema.put(col.getColumnName(), col.getColumnType());
		}
		
		Type type;
		for (int i = 0; i < n; i++) {
			int j = 0;
			/*content = new Object[mapResult.size()];*/
			content = new HashMap<String, Object>();
			
			for (Column col : mapResult.values()) {
				type = col.getColumnType();
				
				if (type == Types.getIntegerType()) {
					int[] data = (int[])col.getDataArrayAsObject();
					//boxing
					if (data[i] != Integer.MAX_VALUE)
						/*content[j++] = new Integer(data[i]);*/
						content.put(col.getColumnName(), new Integer(data[i]));
				}
				
				else if (type == Types.getFloatType()) {
					float[] data = (float[])col.getDataArrayAsObject();
					//boxing
					if (data[i] != Float.MAX_VALUE)
						/*content[j++] = new Float(data[i]);*/
						content.put(col.getColumnName(), new Float(data[i]));	
				}
				
				else if (type == Types.getDoubleType()) {
					double[] data = (double[])col.getDataArrayAsObject();
					//boxing
					if (data[i] != Double.MAX_VALUE)
					/*content[j++] = new Double(data[i]);*/
						content.put(col.getColumnName(), new Double(data[i]));
				}
				else if (type == Types.getLongType()) {
					long[] data = (long[])col.getDataArrayAsObject();
					//boxing
					if (data[i] != Long.MAX_VALUE)
						/*content[j++] = new Long(data[i]);*/
						content.put(col.getColumnName(), new Long(data[i]));
				}
				else {
					Object[] data = (Object[])col.getDataArrayAsObject();
					//reference, not copy
					if (data[i] != MyNull.NULLOBJ)
						/*content[j++] = data[i];*/
						content.put(col.getColumnName(), data[i]);
				}
			}
			rowResult.add(new TempRow(schema, content));
		}
		return new MyOperator(rowResult);
	}

	private Map<String,Column> adaptedQuery(RelationalAlgebraExpression query) throws SchemaMismatchException, InvalidPredicateException, NoSuchTableException{

		
		RelationalOperatorType qType=query.getType();
		Map<String,Column> processingResult=new HashMap<String,Column>();
		
		
		if(qType==RelationalOperatorType.INPUT){ 
			try {
				Operator<Column> tableAsCols = (Operator<Column>) storageLayer.getTableByName(((Input)query).getRelationName()).getAllColumns();
			
				tableAsCols.open();
				Column c;
				while((c=tableAsCols.next())!=null){
					processingResult.put(c.getColumnName(), c);
				}
				
				tableAsCols.close();
				return processingResult;
			}
			catch (NoSuchTableException ex) {
				throw new SchemaMismatchException();
			}
		}
		
		else if(qType==RelationalOperatorType.PROJECTION){
			Projection curNode = (Projection)query;	
			RelationalAlgebraExpression input = curNode.getInput();
			try {
				return ProjectionOperator.projectWithDuplicates(adaptedQuery(curNode.getInput()), curNode.getProjectionAttributes());
			} catch (NoSuchColumnException e) {
				throw new SchemaMismatchException();
			}
		}
		else if(qType==RelationalOperatorType.SELECTION){
					
			Selection curNode=(Selection)query;
			RelationalAlgebraExpression input = curNode.getInput();
	
			String relation = (input.getType() == RelationalOperatorType.INPUT) ? ((Input)input).getRelationName() : null;
			
			return SelectionOperator.select(adaptedQuery(input), curNode.getPredicate(), relation, indexLayer);
		
		}
		else if(qType==RelationalOperatorType.JOIN){
			Join join=(Join)query;
			try {
				return JoinOperator.joinSimple(adaptedQuery(join.getLeftInput()), adaptedQuery(join.getRightInput()), join.getLeftJoinAttribute(), join.getRightJoinAttribute());
			} catch (NoSuchColumnException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		} 
		else{ // for cross product
			CrossProduct curNode = (CrossProduct)query;
			RelationalAlgebraExpression leftInput = curNode.getLeftInput();
			RelationalAlgebraExpression rightInput = curNode.getRightInput();
			
			return CrossProductOperator.product(adaptedQuery(leftInput), adaptedQuery(rightInput));
		}
	
	}

	@Override
	public String explain(RelationalAlgebraExpression query)
	throws NoSuchTableException, SchemaMismatchException, NoSuchColumnException, 
	InvalidPredicateException{
		// TODO Auto-generated method stub
		return null;
	}
}
