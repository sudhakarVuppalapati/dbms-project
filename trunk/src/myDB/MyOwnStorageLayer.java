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
import exceptions.NoSuchColumnException;
import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;
import exceptions.TableAlreadyExistsException;
import systeminterface.Column;
import systeminterface.StorageLayer;
import systeminterface.Table;

public class MyOwnStorageLayer implements StorageLayer, Serializable {

	/**
	 * Generated serial version UID for serialization. Never used though 
	 */
	private static final long serialVersionUID = -9021652804266678342L;

	private static final String DELIM = "/";

	private transient final HashMap<String, Table> tables;

	public MyOwnStorageLayer() {
		tables = new HashMap<String, Table>();
		/*tableObj = new TablesDescriptor();*/
	}

	@Override
	public Table createTable(String tableName, Map<String, Type> schema)
	throws TableAlreadyExistsException {
		return null;
	}

	@Override
	public void deleteTable(String tableName) throws NoSuchTableException {
		// TODO Auto-generated method stub

	}

	@Override
	public Table getTableByName(String tableName) throws NoSuchTableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator<? extends Table> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Might need to integrate with Razvan's code
	 */  
	@Override
	public Operator<? extends Table> loadTablesFromExtentIntoMainMemory()
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
					tmpSchema.put(tmpTblName, tmpType);
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

					/** Read the first line, which contains info about the
					 * first column. Construct the column together with rows  */
					if (tc.colNum > 0) {						
						tmpStr = tableColInfo.get(tblItem).get(0); 
						mc = (MyColumn) tmpTbl.getColumnByName(tmpStr);
						tmpType = mc.getColumnType();

						if(tmpType == Types.getIntegerType()) {
							ints = new int[Math.round(tc.rowNum * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								ints[i] = (Integer) tc.data[i];
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}								
							mc.setData(ints, tc.rowNum);
						}

						if(tmpType == Types.getLongType()){
							longs = new long[Math.round(tc.rowNum * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								longs[i] = (Long) tc.data[i];
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(longs, tc.rowNum);
						}

						if(tmpType == Types.getDoubleType()){
							doubles = new double[Math.round(tc.rowNum * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								doubles[i] = (Double) tc.data[i];
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(doubles, tc.rowNum);
						}

						if(tmpType == Types.getFloatType()){
							floats = new float[Math.round(tc.rowNum * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								floats[i] = (Float) tc.data[i];
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(floats, tc.rowNum);
						}

						tmpColList = new ArrayList(Math.round(tc.rowNum * Const.FACTOR));
						for (i = 0; i < tc.rowNum; i++) {
							tmpColList.add(tc.data[i]);
							tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
						}
						mc.setData(tmpColList, tc.rowNum);

						/** Read the other columns */
						for (tmp = 1; tmp < tc.colNum; i++) {
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
					}					
				}
				/** Using DataInputStream. Keep in mind that we store data elements
				 * by columns first, then by rows */
				else {
					dis = new DataInputStream(bis);
					colNo = dis.readInt();
					rowNo = dis.readInt();

					/** Read the first line, which contains info about the
					 * first column. Construct the column together with rows  */
					if (colNo > 0) {
						tmpStr = tableColInfo.get(tblItem).get(0); 
						mc = (MyColumn) tmpTbl.getColumnByName(tmpStr);
						tmpType = mc.getColumnType();

						if(tmpType == Types.getIntegerType()) {
							ints = new int[Math.round(rowNo * Const.FACTOR)];
							for (i = 0; i < rowNo; i++) {
								ints[i] = dis.readInt();
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}								
							mc.setData(ints, rowNo);
						}

						if(tmpType == Types.getLongType()){
							longs = new long[Math.round(rowNo * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								longs[i] = dis.readLong();
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(longs, rowNo);
						}

						if(tmpType == Types.getDoubleType()){
							doubles = new double[Math.round(rowNo * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								doubles[i] = dis.readDouble();
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(doubles, rowNo);
						}

						if(tmpType == Types.getFloatType()){
							floats = new float[Math.round(rowNo * Const.FACTOR)];
							for (i = 0; i < tc.rowNum; i++) {
								floats[i] = dis.readFloat();
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(floats, rowNo);
						}

						if(tmpType == Types.getDateType()){
							tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
							for (i = 0; i < tc.rowNum; i++) {
								tmpColList.add(Date.parse(dis.readUTF()));
								tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
							}
							mc.setData(tmpColList, rowNo);
						}

						tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
						for (i = 0; i < tc.rowNum; i++) {
							tmpColList.add(dis.readUTF());
							tmpTbl.addRow(new MyRow(tmpSchema, tmpTbl, i));
						}
						mc.setData(tmpColList, rowNo);

						/** Read the other columns */
						for (tmp = 1; tmp < tc.colNum; i++) {
							tmpStr = tableColInfo.get(tblItem).get(0); 
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
								for (i = 0; i < tc.rowNum; i++)
									longs[i] = dis.readLong();
								mc.setData(longs, rowNo);
								continue;
							}

							if(tmpType == Types.getDoubleType()){
								doubles = new double[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++)
									doubles[i] = dis.readDouble();
								mc.setData(doubles, rowNo);
								continue;
							}

							if(tmpType == Types.getFloatType()){
								floats = new float[Math.round(rowNo * Const.FACTOR)];
								for (i = 0; i < tc.rowNum; i++)
									floats[i] = dis.readFloat();
								mc.setData(floats, rowNo);
								continue;
							}

							if(tmpType == Types.getDateType()){
								tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
								for (i = 0; i < tc.rowNum; i++) 
									tmpColList.add(Date.parse(dis.readUTF()));
								mc.setData(tmpColList, rowNo);
								continue;
							}

							tmpColList = new ArrayList(Math.round(rowNo * Const.FACTOR));
							for (i = 0; i < tc.rowNum; i++)
								tmpColList.add(dis.readUTF());
							mc.setData(tmpColList, rowNo);			
						}
					}				
				}
			}
		}
		catch (NoSuchColumnException nsce) {
			throw new IOException();
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
	}

	@Override
	public void writeTablesFromMainMemoryBackToExtent (
			Operator<? extends Table> table) throws IOException {

		boolean empty = true;
		int i = 0, size = 0;
		String tmpName = null;

		List<Object> data = null;
		Operator<Column> columns = null;
		Column mc = null;
		Type tmpType = null;
		Table t = null;
		Object obj = null;

		/** All possible arrays, to speed up writing */
		double[] doubles = null;
		int[] ints = null;
		float[] floats = null;
		long[] longs = null;

		File[] file = null;
		FileOutputStream fos = null; 
		BufferedOutputStream bos = null;
		ObjectOutputStream oos = null; 
		DataOutputStream dos = null;

		DataOutputStream metaOutput = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(MyPersistentExtent.TABLES_METADATA_FILE)));

		table.open();

		try {
			while ((t = table.next()) != null) {
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
				 * or DataInputStream. As an experiment, DataInputStream is
				 * better for small set of data, while ObjectInputStream pays
				 * off for large set. */						
				if (size > TableContent.THRESHOLD) {
					oos = new ObjectOutputStream(bos);
					data = new ArrayList();

					columns = (Operator<Column>) t.getAllColumns();
					columns.open();
					while ((mc = columns.next()) != null) {
						tmpType = mc.getColumnType();
						/** Write to the meta-data the info of current column */
						metaOutput.writeUTF(mc.getColumnName());
						metaOutput.writeInt(Const.getNumber(tmpType));

						/** Iterate over the rows, write back to storage
						 * device if the data cell is not marked as deleted */
						if(tmpType == Types.getLongType()) {
							for (i = 0; i < mc.getRowCount(); i++) {
								obj = mc.getElement(i);
								if ((Long)obj != Long.MAX_VALUE) {
									data.add(obj);
								}	
							}
							continue;
						}
						if(tmpType == Types.getDoubleType()) {
							for (i = 0; i < mc.getRowCount(); i++) {
								obj = mc.getElement(i);
								if ((Double)obj != Double.MAX_VALUE) {
									data.add(obj);
								}	
							}
							continue;
						}
						if(tmpType == Types.getIntegerType()) {
							for (i = 0; i < mc.getRowCount(); i++) {
								obj = mc.getElement(i);
								if ((Integer)obj != Integer.MAX_VALUE) {
									data.add(obj);
								}	
							}
							continue;
						}
						if(tmpType == Types.getFloatType()) {
							for (i = 0; i < mc.getRowCount(); i++) {
								obj = mc.getElement(i);
								if ((Float)obj != Float.MAX_VALUE) {
									data.add(obj);
								}	
							}
							continue;
						}
						/** If the column is of object type, iterate over rows
						 * and write elements which have MyNull value */
						for (i = 0; i < mc.getRowCount(); i++) {
							obj = mc.getElement(i);
							if (obj != MyNull.NULLOBJ) {
								data.add(obj);
							}	
						}
					}
					columns.close();
					oos.writeObject(new TableContent(data.toArray()));
					oos.close();
				}
				else {
					dos = new DataOutputStream(bos);
					columns = (Operator<Column>) t.getAllColumns();
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

						if(tmpType == Types.getDateType()){
							for (i = 0; i < mc.getRowCount(); i++) {
								obj = mc.getElement(i);
								if (obj != MyNull.NULLOBJ) {
									dos.writeUTF(((Date)obj).toString());
								}
							}
							continue;
						}

						for (i = 0; i < mc.getRowCount(); i++) {
							obj = mc.getElement(i);
							if (obj != MyNull.NULLOBJ) {
								dos.writeUTF(obj.toString());
							}	
						}	
					}
					columns.close();
					dos.close();
					metaOutput.close();
				}
			}
		}
		catch (SchemaMismatchException sme) {
			throw new IOException();
		}
		catch (NoSuchRowException nsre) {
			throw new IOException();
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

	private final String buildFileName(String input) {
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

		TableContent(Object[] data2) {
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

	public static void main(String[] args) {
		StorageLayer storageLayer = new MyOwnStorageLayer();
		try {
			storageLayer.writeTablesFromMainMemoryBackToExtent(new MyOperator<Table>());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done.");
	}
}