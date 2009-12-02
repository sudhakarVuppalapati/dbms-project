/**
 * 
 */
package myDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import metadata.Type;
import metadata.Types;
import operator.Operator;
import exceptions.ColumnAlreadyExistsException;
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
	private List <Row> rows;
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
	
	public Map<String, Type> getTableSchema() {
		return this.schema;
	}

	//to be re-written without all the stupid comparison  of type
	@Override
	public void addColumn(String columnName, Type columnType)
	throws ColumnAlreadyExistsException {

		schema.put(columnName, columnType);

		if (!cols.containsKey(columnName)) {

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

			if(columnType == Types.getFloatType()){
				cols.put(columnName,new MyFloatColumn(columnName,columnType));
				return;
			}

			cols.put(columnName,new MyObjectColumn(columnName,columnType));
		}
		else 
			throw new ColumnAlreadyExistsException();
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
		
		
		//add the row to the rows and the add to each column its correspondent values
		rows.add(row);
		//MyHelper.printArr(colNames);
		MyColumn curCol;
		for(int i=0;i<colNames.length;i++){
				curCol=((MyColumn)cols.get(colNames[i]));
				if(curCol!=null) curCol.add(tmpRowValues[i]);
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
		boolean found=false;
		Object rowCell=null;
		int i=0;
		for(;i<rows.size();i++){
			r=rows.get(i);
			for(int j=0;j<colNames.length;j++){
				try{
					if(!r.getColumnValue(colNames[j]).equals(tmpRowValues[j])){
						//throw new NoSuchRowException();
						break;
					}
					found=true;
				}
				catch(NoSuchColumnException nsce){
					throw new SchemaMismatchException();
				}
			}
			if(found) break;
		}
		
		if(found){
			rows.remove(i); //think about this: if it makes sense to have this "rows" data structure;
			Operator<Column> columns=getAllColumns();
			columns.open();
			Column curCol;
			while((curCol=columns.next())!=null){
				((MyColumn)curCol).remove(i);
			}
		}
		else throw new NoSuchRowException();
	}

	@Override
	public void deleteRow(int tupleID) throws NoSuchRowException {

		if(tupleID>=rows.size()){
			throw new NoSuchRowException();
		}
		Operator<Column> columns=getAllColumns();
		columns.open();
		Column curCol;
		while((curCol=columns.next())!=null){
			((MyColumn)curCol).remove(tupleID);
		}
		//rows.remove(tupleID); //think about this: if it makes sense to have this "rows" data structure;
	}

	@Override
	public void dropColumnByName(String columnName)
	throws NoSuchColumnException {
		if(! cols.containsKey(columnName)){
			throw new NoSuchColumnException();
		}
		cols.remove(columnName);
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
		Operator<Row> opRow = new MyOperator<Row>(rows);
		return opRow;
	}

	/* (non-Javadoc)
	 * @see systeminterface.Table#getRows(systeminterface.PredicateTreeNode)
	 */
	@Override
	public Operator<Row> getRows(PredicateTreeNode predicate)
	throws SchemaMismatchException {
		
		Operator o;
		
		if(predicate==null)
			return new MyOperator(rows);
		
		if(predicate.isLeaf()){
			try{
				Column c=getColumnByName(predicate.getColumnName());
			}
			catch(NoSuchColumnException nsce){
				throw new SchemaMismatchException();
			} catch (NotLeafNodeException e) {
				e.printStackTrace();
			}
			
		}
		return null;
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

		//Operator<Column> columns=getAllColumns();
		
		if(tupleID >= rows.size()) throw new NoSuchRowException();
				/* rows.get(tupleID)!=null */
		
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
		
		//MyHelper.printTable(this);
		
		MyHelper.printRow(oldRow);
		System.out.println("-----");
		MyHelper.printRow(newRow);
		
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
		
		//search the table for the row that is identical to the row to be deleted
		Row r=null;
		boolean found=false;
		Object rowCell=null;
		int i=0;
		for(;i<rows.size();i++){
			r=rows.get(i);
			for(int j=0;j<colNames.length;j++){
				try{
					//System.out.println("Comparing "+r.getColumnValue(colNames[j])+" and "+tmpRowValues[j]);
					if(!r.getColumnValue(colNames[j]).equals(oldRow.getColumnValue(colNames[j]))){
						//throw new NoSuchRowException();
						break;
					}
					found=true;
				}
				catch(NoSuchColumnException nsce){
					throw new SchemaMismatchException();
				}
			}
			if(found) break;
		}
		
		if(found){
			//rows.remove(i); //think about this: if it makes sense to have this "rows" data structure;
			System.out.println("updating ----->>>>>>>> updating");
			Operator<Column> columns=getAllColumns();
			Column curCol;
			columns.open();
			int j=0;//cur col in the row
			while((curCol=columns.next())!=null){
				((MyColumn)curCol).update(i,tmpRowValues[j]);
				j++;
			}
		}
		else throw new NoSuchRowException();
	}
	
	public int getSize() {
		int i = 0, j = 0;
		while () {
			if 
		}
	}

	
}
