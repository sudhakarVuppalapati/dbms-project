/**
 * 
 */
package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import metadata.Type;
import metadata.Types;
import operator.Operator;
import exceptions.ColumnAlreadyExistsException;
import exceptions.IsLeafException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NotLeafNodeException;
import exceptions.SchemaMismatchException;
import firstmilestone.MyHelper;
import systeminterface.Column;
import systeminterface.PersistentExtent;
import systeminterface.PredicateTreeNode;
import systeminterface.Row;
import systeminterface.Table;
import util.ComparisonOperator;
import util.LogicalOperator;

/**
 * @author razvan
 *
 */
public class MyTable implements Table {

	/* (non-Javadoc)
	 * @see systeminterface.Table#addColumn(java.lang.String, metadata.Type)
	 */

	private String name;
	private Map<String, Type> schema;
	private List<Row> rows;
	private Map<String, Column> cols;


	public MyTable(String tableName, Map<String,Type> tableSchema){
		name=tableName;
		schema=tableSchema;
		rows=new ArrayList<Row>();
		cols=new HashMap<String,Column>();

		/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!    */
		Set<String> colNames=schema.keySet();
		Type colType;
		Column col;
		for(String colName:colNames){
			colType=schema.get(colName);

			if(colType == Types.getIntegerType()){
				col=new MyIntColumn(colName,colType);
			}
			else if(colType == Types.getDoubleType()){
				col=new MyDoubleColumn(colName,colType);
			}
			else if(colType == Types.getFloatType()){
				col=new MyFloatColumn(colName,colType);
			}
			else if(colType == Types.getLongType()){
				col=new MyLongColumn(colName,colType);
			}
			else col=new MyObjectColumn(colName,colType);

			cols.put(colName,col);
		}
	}

	public Map<String, Type> getTableSchema(){
		return this.schema;
	}


	//to be re-written without all the stupid comparison  of type
	@Override
	public void addColumn(String columnName, Type columnType)
	throws ColumnAlreadyExistsException {

		if(schema.containsKey(columnName)) throw new ColumnAlreadyExistsException();

		schema.put(columnName, columnType);

		if(columnType == Types.getIntegerType()){
			cols.put(columnName,new MyIntColumn(columnName,columnType));
			return;
		}

		if(columnType == Types.getLongType()){
			cols.put(columnName,new MyLongColumn(columnName,columnType));
			return;
		}

		if(columnType == Types.getDoubleType()){
			cols.put(columnName,new MyDoubleColumn(columnName,columnType));
			return;
		}

		if(columnType== Types.getFloatType()){
			cols.put(columnName,new MyFloatColumn(columnName,columnType));
			return;
		}

		cols.put(columnName,new MyObjectColumn(columnName,columnType));


	}

	/*public boolean checkSchema_v2(Row row){

		String[] colNames=row.getColumnNames();

		//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			return false;
		}

		//check if the name of the columns are the same
		String[] schemaColNames=(String[])schema.keySet().toArray();
		boolean found=false;
		for(int i=0;i<schemaColNames.length;i++){
			for(int j=0;j<colNames.length;j++)
				if(schemaColNames[i].equalsIgnoreCase(colNames[j])) {
					found=true;
					break;
				}
			if(!found){
				return false; //the schema lacks at least one column name
			}
		}

		//check the actual types
		for(int j=0;j<colNames.length;j++){
			try{
				if(! row.getColumnType(colNames[j]).equals(schema.get(colNames[j]))){
					return false;
				}
			}
			catch(NoSuchColumnException nsce){
				return false;
			}
		}
	}

	public boolean checkSchema(Row row){

		String[] colNames=row.getColumnNames();

		//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			return false;
		}

		//check there is no duplicate in the row to be added
		HashMap<String,Type> colMap=new HashMap<String,Type>();
		for(int i=0;i<colNames.length;i++){
			try{
				colMap.put(colNames[i],row.getColumnType(colNames[i]));
			}
			catch(NoSuchColumnException nsce){
				return false;
			}
		}
		if(colMap.size()!=schema.size())
			return false;




		//check the actual types
		Type curRowType, curSchemaType;
		for(int i=0;i<colNames.length;i++){
			curSchemaType = schema.get(colNames[i]);

			if(curSchemaType==null) return false;

			curRowType=colMap.get(colNames[i]);

			if(!curSchemaType.equals(curRowType))
				return false;
		}
	}*/

	/* (non-Javadoc)
	 * @see systeminterface.Table#addRow(systeminterface.Row)
	 */
	@Override
	public int addRow(Row row) throws SchemaMismatchException {
		String[] colNames=row.getColumnNames();
		Object[] tmpRowValues=new Object[colNames.length];

		/*//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			throw new SchemaMismatchException();
		}

		//check if the name of the columns are the same
		//String[] schemaColNames=(String[])schema.keySet().toArray(new String[0]);
		boolean found=false;
		for(int i=0;i<schemaColNames.length;i++){
			for(int j=0;j<colNames.length;j++)
				if(schemaColNames[i].equalsIgnoreCase(colNames[j])) {
					found=true;
					break;
				}
			if(!found){
				throw new SchemaMismatchException(); //the schema lacks at least one column name
			}
		}
		
		for(int i=0; i< colNames.length;i++)
			if(!schema.containsKey(colNames[i])) throw new SchemaMismatchException();
		
		
		//check the actual types
		for(int j=0;j<colNames.length;j++){
			try{
				if(!row.getColumnType(colNames[j]).equals(schema.get(colNames[j]))){
					throw new SchemaMismatchException();
				}
				tmpRowValues[j]=row.getColumnValue(colNames[j]);
			}
			catch(NoSuchColumnException nsce){
				throw new SchemaMismatchException();
			}
		}*/
		
		try{
			for(int j=0;j<colNames.length;j++)
				tmpRowValues[j]=row.getColumnValue(colNames[j]);
		
			
			Row newRow=new MyRow(this,rows.size());
	
			//add the row to the rows and then add to each column its correspondent value
			rows.add(newRow);
			
			MyColumn curCol;
			for(int i=0;i<colNames.length;i++){
				curCol=((MyColumn)cols.get(colNames[i]));
				if(curCol!=null) curCol.add(tmpRowValues[i]);
			}
		}
		catch(Exception nsce){
			throw new SchemaMismatchException();
		}
		
		return rows.size()-1;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#assignExtent(systeminterface.PersistentExtent)
	 */
	@Override
	public void assignExtent(PersistentExtent extent) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#deleteRow(systeminterface.Row)
	 */
	@Override
	public void deleteRow(Row row) throws NoSuchRowException,
	SchemaMismatchException {

		String[] colNames=row.getColumnNames();
		Object[] tmpRowValues=new Object[colNames.length];

		//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			throw new SchemaMismatchException();
		}

		//check if the name of the columns are the same
		String[] schemaColNames=(String[])schema.keySet().toArray(new String[0]);
		boolean col_found=false;
		for(int i=0;i<schemaColNames.length;i++){
			for(int j=0;j<colNames.length;j++)
				if(schemaColNames[i].equalsIgnoreCase(colNames[j])) {
					col_found=true;
					break;
				}
			if(!col_found){
				throw new SchemaMismatchException(); //the schema lacks at least one column name
			}
		}

		//check the actual types
		for(int j=0;j<colNames.length;j++){
			try{
				if(! row.getColumnType(colNames[j]).equals(schema.get(colNames[j]))){
					throw new SchemaMismatchException();
				}

				tmpRowValues[j]=row.getColumnValue(colNames[j]);
			}
			catch(NoSuchColumnException nsce){
				throw new SchemaMismatchException();
			}
		}

		//search the table for the row that is identical to the row to be deleted
		Row r=null;
		
		boolean found=false,deleted=false;
		Object rowCell=null,oldRowCell=null;
		int i=0;
		for(;i<rows.size() ;i++){
			r=rows.get(i);
			if(r!=null){
				found=true;
				for(int j=0;j<colNames.length;j++){
					try{
						rowCell=r.getColumnValue(colNames[j]);
						oldRowCell=row.getColumnValue(colNames[j]);
						if(rowCell==null && oldRowCell!=null){
							found=false;
							break;
						}
						if(!rowCell.equals(oldRowCell)){
							found=false;
							break;
						}
					}
					catch(NoSuchColumnException nsce){
						//found=false;
						throw new SchemaMismatchException();
					}
				}
				if(found) {
					deleted=true;
					//deleteIds.add(new Integer(i));
					//System.out.println("Deleted: ");
					//MyHelper.printRow(rows.get(i));
					rows.set(i, null); //we finally decided to do it this way but thik about it
					
					/*Operator<Column> columns=getAllColumns();
					columns.open();
					Column curCol;
					while((curCol=columns.next())!=null){
						((MyColumn)curCol).remove(i);
					}*/
					Column col;
					for(int k=0;k<colNames.length;k++){
						col=cols.get(colNames[k]);
						((MyColumn)col).remove(i);
					}
				}
			}
		}
		
		if(!deleted)
			throw new NoSuchRowException();
	}

	@Override
	public void deleteRow(int tupleID) throws NoSuchRowException {
		if(tupleID>=rows.size())
			throw new NoSuchRowException();
		
		if(rows.get(tupleID)==null)
			throw new NoSuchRowException();
		
		
		String[] cNames=(String[])cols.keySet().toArray(new String[0]);
		for(int p=0;p<cNames.length;p++)
			((MyColumn)cols.get(cNames[p])).remove(tupleID);
		
		rows.set(tupleID,null); // we finally decided to do it this way but think about it
		
	}

	@Override
	public void dropColumnByName(String columnName)
	throws NoSuchColumnException {
		Type t=schema.remove(columnName);
		if(t!=null){
			cols.remove(columnName);
			return;
		}
		
		throw new NoSuchColumnException();
		
		/*if(!cols.containsKey(columnName)){
			throw new NoSuchColumnException();
		}
		schema.remove(columnName);
		cols.remove(columnName);*/
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getAllColumns()
	 */
	@Override
	public Operator<Column> getAllColumns() {
		Operator<Column> opCol = new MyOperator<Column>(cols.values());
		return opCol;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getColumnByName(java.lang.String)
	 */
	@Override
	public Column getColumnByName(String columnName)
	throws NoSuchColumnException {
		Column col = cols.get(columnName);
		if (col != null)
			return col;

		throw new NoSuchColumnException();

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getColumns(java.lang.String[])
	 */
	@Override
	public Operator<Column> getColumns(String... columnNames)
	throws NoSuchColumnException {				

		List<Column> colList=new ArrayList<Column>();
		for (String tmp : columnNames) {
			colList.add(cols.get(tmp));
		}

		return new MyOperator<Column>(colList);
	}

	
	/* (non-Javadoc)
	 * @see systeminterface.Table#getRows()
	 */
	@Override
	public Operator<Row> getRows() {
		//filter out the null and than return the resulting operator
		List<Row> filteredRows=new ArrayList();
		Row r=null;
		for(int i=0;i<rows.size();i++)
			if((r=rows.get(i))!=null)
				filteredRows.add(r);
		
		Operator<Row> opRow = new MyOperator<Row>(filteredRows);
		return opRow;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getRows(systeminterface.PredicateTreeNode)
	 */
	@Override
	public Operator<Row> getRows(PredicateTreeNode predicate)
	throws SchemaMismatchException {
		/*
		Operator<Row> op1=null,op2=null;
		
		
		LogicalOperator lp=null;
		ComparisonOperator co;
		Row r,r1;
		try{
		if(!predicate.isLeaf()){
			
			PredicateTreeNode leftChild=predicate.getLeftChild();
			PredicateTreeNode rightChild=predicate.getRightChild();
			
			if(leftChild!=null)
				op1=getRows(leftChild);
			if(rightChild!=null)
				op2=getRows(rightChild);
			
			lp=predicate.getLogicalOperator();
			
			if(lp==LogicalOperator.OR){
				
				Map result=new HashMap();
				
				op1.open();
				op2.open();
				
				while((r=op1.next())!=null){
					result.put(r,null);
				}
				
				while((r=op2.next())!=null){
					result.put(r,null);
				}
				
				op1.close();
				op2.close();
				
				return new MyOperator<Row>(result.keySet());
			}
			else{
				List resultList=new ArrayList();
				while((r=op1.next())!=null)
					while((r1=op2.next())!=null)
						if(r==r1) resultList.add(r1);
				return new MyOperator<Row>(resultList);	
			}
		}
		else {
			
			Object colValue=null;
			Type columnType=null;
			List<Row> filteredRows=new ArrayList<Row>();
			for(int i=0;i<rows.size();i++){
				co=predicate.getComparisonOperator();
				r=rows.get(i);
				Object value=predicate.getValue();
				String attr=predicate.getColumnName();
				if(r!=null){
					colValue=r.getColumnValue(attr);
					
					if(co==ComparisonOperator.EQ){
						if(colValue==null && value==null) filteredRows.add(r);
						else if(colValue.equals(value)) filteredRows.add(r);
					}
					if(co==ComparisonOperator.GEQ){
						if(colValue!=null && value!=null){
							if(columnType == Types.getIntegerType()){
								if(((Integer)colValue)>=((Integer)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getLongType()){
								if(((Long)colValue)>=((Long)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getDoubleType()){
								if(((Double)colValue)>=((Double)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType== Types.getFloatType()){
								if(((Float)colValue)>=((Float)value)){
									filteredRows.add(r);
								}
							}
							
							else if (columnType== Types.getDateType()){
								if(((Date)colValue).compareTo((Date)value)>=0){
									filteredRows.add(r);
								}
							}
							else{
								if(((String)colValue).compareTo((String)value)>=0)
									filteredRows.add(r);
							}
							
						}
					}
					
					if(co==ComparisonOperator.LEQ){
						if(colValue!=null && value!=null){
							if(columnType == Types.getIntegerType()){
								if(((Integer)colValue)<=((Integer)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getLongType()){
								if(((Long)colValue)<=((Long)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getDoubleType()){
								if(((Double)colValue)<=((Double)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType== Types.getFloatType()){
								if(((Float)colValue)<=((Float)value)){
									filteredRows.add(r);
								}
							}
							
							else if (columnType== Types.getDateType()){
								if(((Date)colValue).compareTo((Date)value)<=0){
									filteredRows.add(r);
								}
							}
							else{
								if(((String)colValue).compareTo((String)value)<=0)
									filteredRows.add(r);
							}
							
						}
					}
					
					
					if(co==ComparisonOperator.GT){
						if(colValue!=null && value!=null){
							if(columnType == Types.getIntegerType()){
								if(((Integer)colValue)>((Integer)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getLongType()){
								if(((Long)colValue)>((Long)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getDoubleType()){
								if(((Double)colValue)>((Double)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType== Types.getFloatType()){
								if(((Float)colValue)>((Float)value)){
									filteredRows.add(r);
								}
							}
							
							else if (columnType== Types.getDateType()){
								if(((Date)colValue).compareTo((Date)value)>0){
									filteredRows.add(r);
								}
							}
							else{
								if(((String)colValue).compareTo((String)value)>0)
									filteredRows.add(r);
							}
							
						}
					}
					
					if(co==ComparisonOperator.LT){
						if(colValue!=null && value!=null){
							if(columnType == Types.getIntegerType()){
								if(((Integer)colValue)<((Integer)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getLongType()){
								if(((Long)colValue)<((Long)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getDoubleType()){
								if(((Double)colValue)<((Double)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType== Types.getFloatType()){
								if(((Float)colValue)<((Float)value)){
									filteredRows.add(r);
								}
							}
							
							else if (columnType== Types.getDateType()){
								if(((Date)colValue).compareTo((Date)value)<0){
									filteredRows.add(r);
								}
							}
							else{
								if(((String)colValue).compareTo((String)value)<0)
									filteredRows.add(r);
							}
							
						}
					}
					
					if(co==ComparisonOperator.NEQ){
						if(colValue!=null && value!=null){
							if(columnType == Types.getIntegerType()){
								if(((Integer)colValue)!=((Integer)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getLongType()){
								if(((Long)colValue)!=((Long)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType == Types.getDoubleType()){
								if(((Double)colValue)!=((Double)value)){
									filteredRows.add(r);
								}
							}

							else if(columnType== Types.getFloatType()){
								if(((Float)colValue)!=((Float)value)){
									filteredRows.add(r);
								}
							}
							
							else if (columnType== Types.getDateType()){
								if(((Date)colValue).compareTo((Date)value)!=0){
									filteredRows.add(r);
								}
							}
							else{
								if(((String)colValue).compareTo((String)value)!=0)
									filteredRows.add(r);
							}
							
						}
					}
					
				}
			}
			return new MyOperator(filteredRows);
		}
		}
		catch (Exception e) {
			e.printStackTrace();
			return new MyOperator();
		}*/return null;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getTableName()
	 */
	@Override
	public String getTableName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#renameColumn(java.lang.String, java.lang.String)
	 */
	@Override
	public void renameColumn(String oldColumnName, String newColumnName)
	throws ColumnAlreadyExistsException, NoSuchColumnException {

		Column col=cols.remove(oldColumnName);
		if(col!=null)
			if(!cols.containsKey(newColumnName)){
				cols.put(newColumnName, col);
			}
			else throw new ColumnAlreadyExistsException();

		throw new NoSuchColumnException();

	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#updateRow(int, systeminterface.Row)
	 */
	@Override
	public void updateRow(int tupleID, Row row)
	throws SchemaMismatchException, NoSuchRowException {

		String[] colNames=row.getColumnNames();
		Object[] tmpRowValues=new Object[colNames.length];
		
		
		if(tupleID >= rows.size()) throw new NoSuchRowException();
		
		if(rows.get(tupleID)==null) throw new NoSuchRowException();
		
		//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			throw new SchemaMismatchException();
		}

		//check if the name of the columns are the same
		String[] schemaColNames=(String[])schema.keySet().toArray(new String[0]);
		boolean found=false;
		for(int i=0;i<schemaColNames.length;i++){
			for(int j=0;j<colNames.length;j++)
				if(schemaColNames[i].equalsIgnoreCase(colNames[j])) {
					found=true;
					break;
				}
			if(!found){
				throw new SchemaMismatchException(); //the schema lacks at least one column name
			}
		}

		//check the actual types
		for(int j=0;j<colNames.length;j++){
			try{
				if(! row.getColumnType(colNames[j]).equals(schema.get(colNames[j]))){
					throw new SchemaMismatchException();
				}

				tmpRowValues[j]=row.getColumnValue(colNames[j]);
			}
			catch(NoSuchColumnException nsce){
				throw new SchemaMismatchException();
			}
		}
		
		Column col;
		for(int i=0;i<colNames.length;i++){
			col=cols.get(colNames[i]);
			((MyColumn)col).update(tupleID,tmpRowValues[i]);
		}
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#updateRow(systeminterface.Row, systeminterface.Row)
	 */
	@Override
	public void updateRow(Row oldRow, Row newRow)
	throws SchemaMismatchException, NoSuchRowException {
		String[] colNames=newRow.getColumnNames();
		Object[] tmpRowValues=new Object[colNames.length];
		
		//check size of the new rowSchema
		if(colNames.length!=schema.size()){
			throw new SchemaMismatchException();
		}

		//check if the name of the columns are the same
		String[] schemaColNames=(String[])schema.keySet().toArray(new String[0]);
		boolean col_found=false;
		for(int i=0;i<schemaColNames.length;i++){
			for(int j=0;j<colNames.length;j++)
				if(schemaColNames[i].equalsIgnoreCase(colNames[j])) {
					col_found=true;
					break;
				}
			if(!col_found){
				throw new SchemaMismatchException(); //the schema lacks at least one column name
			}
		}

		//check the actual types
		for(int j=0;j<colNames.length;j++){
			try{
				if(!newRow.getColumnType(colNames[j]).equals(schema.get(colNames[j]))){
					throw new SchemaMismatchException();
				}

				tmpRowValues[j]=newRow.getColumnValue(colNames[j]);
			}
			catch(NoSuchColumnException nsce){
				throw new SchemaMismatchException();
			}
		}

		//search the table for the row that is identical to the row to be updated
		Row r=null;
		boolean found=false,updated=false;
		Object rowCell=null,oldRowCell=null;
		int i=0;
		for(;i<rows.size();i++){
			r=rows.get(i);
			if(r!=null){
				found=true;
				for(int j=0;j<colNames.length;j++){
					try{
						rowCell=r.getColumnValue(colNames[j]);
						oldRowCell=oldRow.getColumnValue(colNames[j]);
						if(rowCell==null && oldRowCell!=null){
							found=false;
							break;
						}
						if(!rowCell.equals(oldRowCell)){
							found=false;
							break;
						}
					}
					catch(NoSuchColumnException nsce){
						throw new SchemaMismatchException();
					}
				}
				if(found) {
					updated=true;
					
					Column col;
					for(int k=0;k<colNames.length;k++){
						col=cols.get(colNames[k]);
						((MyColumn)col).update(i,tmpRowValues[k]);
					}
				}
			}
			
		}
		
		if(!updated)
			throw new NoSuchRowException();
	}

	public int getSize() {
		int i = 0, j = 0;
		while (j < rows.size()) {
			if (rows.get(j++) == null) i++;
		}
		return i * cols.size();
	}
	
	public Row getRow(int i){
		return rows.get(i);
	}
}
