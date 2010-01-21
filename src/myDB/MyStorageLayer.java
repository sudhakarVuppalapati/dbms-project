package myDB;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metadata.Type;
import metadata.Types;
import operator.Operator;
import systeminterface.Column;
import systeminterface.Row;
import systeminterface.StorageLayer;
import systeminterface.Table;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;

/**
 * The storage layer of the database. Implement this skeleton!!!
 * 
 * 
 */
public class MyStorageLayer implements StorageLayer,Serializable {

	private Map<String,Table> tables;

	private static final String DELIM = "/";

	private static final long serialVersionUID = -9021652804266678342L;
	/**
	 * Constructor,
	 */
	public MyStorageLayer() {
		tables = new HashMap<String,Table>();
	}

	@Override
	public Table createTable(String tableName, Map<String, Type> schema)
	throws TableAlreadyExistsException {
		if(tables.containsKey(tableName))
			throw new TableAlreadyExistsException();

		Table tab=new MyTable(tableName,schema);

		tables.put(tableName,tab);

		return tab;
	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		Table t=tables.remove(tableName);
		if(t==null){
			throw new NoSuchTableException();
		}	
	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {
		Table t=tables.get(tableName);
		if(t!=null)
			return t;
		throw new NoSuchTableException();
	}

	@Override
	public Operator<Table> getTables() { 
		// TODO Auto-generated method stub
		return new MyOperator<Table>(tables.values());
	}

	@Override
	public Operator<Table> loadTablesFromExtentIntoMainMemory()
	throws IOException {
		int i = 0, tmp = 0, tmpTyp = 0, tblSize = 0, colNo = 0, rowNo = 0;
		String tmpStr = null, tmpTblName = null;

		boolean newTable = false;

		MyOperator<Table> tableOp = new MyOperator<Table>();
		List<Table> tableElems = new ArrayList<Table>();

		MyColumn mc = null;
		Type tmpType = null;
		MyTable tmpTbl = null;
		Map<String, Type> tmpSchema = null;

		/** All possible arrays, to speedup writing */
		double[] doubles = null;
		int[] ints = null;
		float[] floats = null;
		long[] longs = null;

		ObjectInputStream ois = null;
		DataInputStream dis = null;
		TableContent tc = null;
		BufferedInputStream bis = null;

		Map<String, Integer> tableSizes = new HashMap<String, Integer>();
		Map<String, List<String>> tableColInfo = new HashMap<String, List<String>>();
		List tmpColList = null;

		/**
		 * Step 1: Load metadata file
		 */
		try {
			bis = new BufferedInputStream(new FileInputStream(
					MyPersistentExtent.TABLES_METADATA_FILE));
			dis = new DataInputStream(bis);
			try {
				while(true) {
					tmpStr = dis.readUTF();
					if (tmpStr.equals(DELIM)) {
						newTable = true;
						if (tmpSchema != null) {			
							try {
								tmpTbl = new MyTable(tmpTblName, tmpSchema);
								tableSizes.put(tmpTblName, tblSize);
								tableColInfo.put(tmpTblName, tmpColList);
								tableElems.add(tmpTbl);
								tables.put(tmpTblName, tmpTbl);							
							} catch (Exception e) {
								throw new IOException();
							}
							tmpSchema = null;	
						}	
						continue;
					}
					tmpTyp = dis.readInt();
					if (newTable) {
						tmpSchema = new HashMap<String, Type>();
						tmpColList = new ArrayList<String>();
						tmpTblName = tmpStr;
						tblSize = tmpTyp;
						newTable = false;
						continue;
					}			
					tmpType = Const.getType(tmpTyp);
					tmpSchema.put(tmpStr, tmpType);
					tmpColList.add(tmpStr);
				}
			}
			catch (EOFException eofe) {
				tableOp = new MyOperator<Table>(tableElems);
			}
			/* FileInputStream fis = new FileInputStream(MyPersistentExtent.TABLES_METADATA_FILE);
			bis = new BufferedInputStream(fis);
			ObjectInputStream ois = new ObjectInputStream(bis);
			tableObj = (TablesDescriptor)ois.readObject();
			ois.close(); */
		}
		/**
		 * If metadata file not found, build a new database
		 */
		catch (FileNotFoundException fnfe) {
			return tableOp;
		}
		catch (SchemaMismatchException sme) {
			throw new IOException();
		}

		/** Step 2: Load table's content files */

		try {
			/** Iteratively read all the table files */
			for (String tblItem : tables.keySet()) {
				tmpTbl = (MyTable)tables.get(tblItem);
				tmpSchema = tmpTbl.getTableSchema();				
				bis = new BufferedInputStream(new FileInputStream(
						buildFileName(tblItem)));

				/** Determine which IO mechanism to use, ObjectInputStream
				 * or DataInputStream. As an experiment, DataInputStream is
				 * better for small set of data, while ObjectInputStream pays
				 * off for large set. */
				if (tableSizes.get(tblItem) > TableContent.THRESHOLD) {
					ois = new ObjectInputStream(bis);	
					tc = (TableContent) ois.readObject();

					if (tc.colNum > 0) {				
						/** Iterate over the columns / lines, construct the corresponding
						 * columns. */
						for (tmp = 0; tmp < tc.colNum; tmp++) {
							tmpStr = tableColInfo.get(tblItem).get(tmp); 
							mc = (MyColumn) tmpTbl.getColumnByName(tmpStr);
							tmpType = mc.getColumnType();

							if(tmpType == Types.getIntegerType()) {
								ints = new int[Math.round(tc.rowNum * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++) 
									ints[i] = (Integer) tc.data[i + tc.rowNum * tmp];				
								mc.setData(ints, tc.rowNum);
								continue;
							}

							if(tmpType == Types.getLongType()){
								longs = new long[Math.round(tc.rowNum * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++) 
									longs[i] = (Long) tc.data[i + tc.rowNum * tmp];
								mc.setData(longs, tc.rowNum);
								continue;
							}

							if(tmpType == Types.getDoubleType()){
								doubles = new double[Math.round(tc.rowNum * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++) 
									doubles[i] = (Double) tc.data[i + tc.rowNum * tmp];
								mc.setData(doubles, tc.rowNum);
								continue;
							}

							if(tmpType == Types.getFloatType()){
								floats = new float[Math.round(tc.rowNum * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++) 
									floats[i] = (Float) tc.data[i + tc.rowNum * tmp];
								mc.setData(floats, tc.rowNum);
								continue;
							}

							tmpColList = new ArrayList(Math.round(tc.rowNum * Const.FACTOR));
							for (i = 0; i < tc.rowNum; i++) 
								tmpColList.add(tc.data[i + tc.rowNum * tmp]);
							mc.setData(tmpColList, tc.rowNum);	
						}

						/** Construct the rows */
						for (i = 0; i < tc.rowNum; i++) {
							tmpTbl.addRow(new MyRow(tmpTbl, i));
						}	
					}					
				}
				/** Using DataInputStream. Keep in mind that we store data elements
				 * by columns first, then by rows */
				else {
					dis = new DataInputStream(bis);
					colNo = dis.readInt();
					rowNo = dis.readInt();

					if (colNo > 0) {

						/** Iterate over the columns / lines, construct the corresponding
						 * columns. */
						for (tmp = 0; tmp < colNo; tmp++) {
							tmpStr = tableColInfo.get(tblItem).get(tmp); 
							mc = (MyColumn) tmpTbl.getColumnByName(tmpStr);
							tmpType = mc.getColumnType();

							if(tmpType == Types.getIntegerType()) {
								ints = new int[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < rowNo; i++)
									ints[i] = dis.readInt();								
								mc.setData(ints, rowNo);
								continue;
							}

							if(tmpType == Types.getLongType()){
								longs = new long[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < rowNo; i++)
									longs[i] = dis.readLong();
								mc.setData(longs, rowNo);
								continue;
							}

							if(tmpType == Types.getDoubleType()){
								doubles = new double[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < rowNo; i++)
									doubles[i] = dis.readDouble();
								mc.setData(doubles, rowNo);
								continue;
							}

							if(tmpType == Types.getFloatType()){
								floats = new float[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < rowNo; i++)
									floats[i] = dis.readFloat();
								mc.setData(floats, rowNo);
								continue;
							}

							if(tmpType == Types.getDateType()){
								tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
								for (i = 0; i < rowNo; i++) {
									tmpColList.add(new Date(dis.readLong()));	
								}

								mc.setData(tmpColList, rowNo);
								continue;
							}

							tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
							for (i = 0; i < rowNo; i++)
								tmpColList.add(dis.readUTF());
							mc.setData(tmpColList, rowNo);			
						}

						/** Construct the rows */
						for (i = 0; i < rowNo; i++) {
							tmpTbl.addRow(new MyRow(tmpTbl, i));
						}					
					}				
				}
			}
		}
		catch (NoSuchColumnException nsce) {
			nsce.printStackTrace();
		}
		catch (SchemaMismatchException sme) {
			throw new IOException();
		}
		catch (FileNotFoundException fnfe) {
			throw new IOException();
		}
		catch (ClassNotFoundException cnfe) {
			throw new IOException();
		}
		return tableOp;
	}

	@Override
	public void renameTable(String oldName, String newName)
	throws TableAlreadyExistsException, NoSuchTableException {

		Table t=tables.remove(oldName);

		if(t==null)
			throw new NoSuchTableException();

		if(tables.containsKey(newName))
			throw new TableAlreadyExistsException();

		((MyTable)t).setTableName(newName);

		tables.put(newName,t);
	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent(
			Operator<? extends Table> table) throws IOException {
		boolean empty = true;
		int i = 0, size = 0, tmpCol = 0, tmpRow = 0;
		String tmpName = null;

		List<Object> data = null;
		Operator<Column> columns = null;
		Operator<Row> rows = null;
		Column mc = null;
		Type tmpType = null;
		Table t = null;
		Object obj = null;

		/** All possible arrays, to speed up writing */
		double[] doubles = null;
		int[] ints = null;
		float[] floats = null;
		long[] longs = null;
		Object[] objects = null;

		File[] file = null;
		FileOutputStream fos = null; 
		BufferedOutputStream bos = null;
		ObjectOutputStream oos = null; 
		DataOutputStream dos = null;
		
		DataOutputStream metaOutput;
		 
		try {
			metaOutput = new DataOutputStream (
					new BufferedOutputStream (new FileOutputStream (
							MyPersistentExtent.TABLES_METADATA_FILE)));

			table.open();

			while ((t = table.next()) != null) {
				tmpCol = tmpRow = 0;
				empty = false;
				tmpName = t.getTableName();

				/** First, write to the meta-file */
				metaOutput.writeUTF(DELIM);
				metaOutput.writeUTF(tmpName);

				/** Might need to check the schema consistency */
				fos = new FileOutputStream(buildFileName(tmpName), false);
				bos = new BufferedOutputStream(fos);

				/** Write the size of the table, or the multiplication of rows and
				 * columns */
				size = getTableSize(t);
				metaOutput.writeInt(size);

				/** Determine which IO mechanism to use, ObjectInputStream
				 * or DataInputStream. As shown in our experiment, 
				 * DataInputStream is better for small set of data, 
				 * while ObjectInputStream pays off for the large sets. */						
				if (size > TableContent.THRESHOLD) {
					oos = new ObjectOutputStream(bos);
					data = new ArrayList();

					columns = (Operator<Column>) t.getAllColumns();

					columns.open();
					while (columns.next() != null)
						tmpCol++;
					columns.close();

					rows = (Operator<Row>) t.getRows();
					rows.open();
					while (rows.next() != null)
						tmpRow++;
					rows.close();

					columns.open();

					while ((mc = columns.next()) != null) {
						tmpType = mc.getColumnType();
						/** Write to the meta-data the info of current column */
						metaOutput.writeUTF(mc.getColumnName());
						metaOutput.writeInt(Const.getNumber(tmpType));

						/** Iterate over the rows, write back to storage
						 * device if the data cell is not marked as deleted */
						if(tmpType == Types.getIntegerType()) {
							ints = (int[])mc.getDataArrayAsObject();
							/** Iterate over the rows, write back to storage
							 * device if the data cell is not marked as deleted */									
							for (i = 0; i < mc.getRowCount(); i++) {
								if (ints[i] != Integer.MAX_VALUE) {
									data.add(ints[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getLongType()){
							longs = (long[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (longs[i] != Long.MAX_VALUE) {
									data.add(longs[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getDoubleType()){
							doubles = (double[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (doubles[i] != Double.MAX_VALUE) {
									data.add(doubles[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getFloatType()){
							floats = (float[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (floats[i] != Float.MAX_VALUE) {
									data.add(floats[i]);
								}	
							}									
							continue;
						}

						if(tmpType == Types.getDateType()) {							
							objects = (Object[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (objects[i] != MyNull.NULLOBJ) {
									data.add(((Date)objects[i]).getTime());
								}	
							}	
							continue;
						}

						objects = (Object[])mc.getDataArrayAsObject();
						for (i = 0; i < mc.getRowCount(); i++) {
							if (objects[i] != MyNull.NULLOBJ) {
								data.add(objects[i].toString());
							}	
						}	

					}
					columns.close();
					oos.writeObject(new TableContent(tmpCol, tmpRow, data.toArray()));
					oos.close();
				}
				else {
					dos = new DataOutputStream(bos);

					columns = (Operator<Column>) t.getAllColumns();

					columns.open();
					while (columns.next() != null)
						tmpCol++;
					columns.close();
					dos.writeInt(tmpCol);

					rows = (Operator<Row>) t.getRows();
					rows.open();
					while (rows.next() != null)
						tmpRow++;
					rows.close();
					dos.writeInt(tmpRow);

					columns.open();
					while ((mc = (MyColumn)columns.next()) != null) {						
						tmpType = mc.getColumnType();

						/** Write to the meta-data the info of current column */
						metaOutput.writeUTF(mc.getColumnName());
						metaOutput.writeInt(Const.getNumber(tmpType));

						if(tmpType == Types.getIntegerType()) {
							ints = (int[])mc.getDataArrayAsObject();
							/** Iterate over the rows, write back to storage
							 * device if the data cell is not marked as deleted */									
							for (i = 0; i < mc.getRowCount(); i++) {
								if (ints[i] != Integer.MAX_VALUE) {
									dos.writeInt(ints[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getLongType()){
							longs = (long[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (longs[i] != Long.MAX_VALUE) {
									dos.writeLong(longs[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getDoubleType()){
							doubles = (double[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (doubles[i] != Double.MAX_VALUE) {
									dos.writeDouble(doubles[i]);
								}	
							}	
							continue;
						}

						if(tmpType == Types.getFloatType()){
							floats = (float[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (floats[i] != Float.MAX_VALUE) {
									dos.writeFloat(floats[i]);
								}	
							}									
							continue;
						}

						if(tmpType == Types.getDateType()) {							
							objects = (Object[])mc.getDataArrayAsObject();
							for (i = 0; i < mc.getRowCount(); i++) {
								if (objects[i] != MyNull.NULLOBJ) {
									dos.writeLong(((Date)objects[i]).getTime());
								}	
							}	
							continue;
						}

						objects = (Object[])mc.getDataArrayAsObject();
						for (i = 0; i < mc.getRowCount(); i++) {
							if (objects[i] != MyNull.NULLOBJ) {
								dos.writeUTF(objects[i].toString());
							}	
						}	
					}
					columns.close();
					dos.close();

				}
				metaOutput.writeUTF(DELIM);
				metaOutput.close();
			}
			table.close();
			
			if (empty) {
				new File(MyPersistentExtent.TABLES_METADATA_FILE).delete();

				file = new File(MyPersistentExtent.DISK).listFiles(
						new FilenameFilter() {
							public boolean accept(File dir, String name) {
								return name.endsWith(MyPersistentExtent.TABLE_EXT);
							}
						});
				for (File fileItem : file)
					fileItem.delete();
			}
		}
		catch (SchemaMismatchException sme) {
			throw new IOException();
		}
		catch (FileNotFoundException fnfe) {
			return;
		}

		
	}

	private static String buildFileName(String input) {
		return new StringBuilder().
		append(MyPersistentExtent.DISK_PREFIX).
		append(input).append(MyPersistentExtent.TABLE_EXT).toString();
	}

	final class TableContent implements Serializable {

		/**
		 * Generated serial version UID for serialization
		 */
		private static final long serialVersionUID = 7935718789696770092L;

		/** Need to estimate later on */
		static transient final int THRESHOLD = 10000; 

		int colNum = 0, rowNum = 0;

		Object[] data;

		TableContent(int col, int row, Object[] data2) {
			colNum = col;
			rowNum = row;
			data = data2;
		}
	}

	private int getTableSize(Table t) {
		if (tables.containsKey(t.getTableName())) {
			try {
				MyTable table = (MyTable)t;
				return table.getSize();
			}
			catch (ClassCastException cce) {
				return Const.DEFAULT_TABLE_SIZE;
			}
		} 
		else return Const.DEFAULT_TABLE_SIZE;
	}

}
