package myDB;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import exceptions.ColumnAlreadyExistsException;
import metadata.Type;
import metadata.Types;

import operator.Operator;
import systeminterface.Table;

public class TablesObject implements Serializable {

	/**
	 * Generated Serial UID
	 */
	private static final long serialVersionUID = 1321413373821042458L;

	/** Store table names together with their columns' names */
	private String[] names;

	/** Store number of tables */
	private int tableNumber;

	/** Store information of column numbers, column types, number of rows */
	private int[] cols;

	/**
	 * Store the actual data. NOTE: If the columns are empty, then a dummy
	 * element MyNull.NULLOBJ will be written out. This is to avoid the
	 * EOFException in ObjectOutputStream
	 */
	private Serializable[] data;

	/** Empty constructor */
	public TablesObject() {
	}

	/** Default constructor */
	public TablesObject(int tblNum, String[] names, int[] cols,
			Serializable[] data) {
		this.names = names;
		this.tableNumber = tblNum;
		this.cols = cols;
		this.data = data;
	}

	public List<Table> buildTables() throws IOException,
			ColumnAlreadyExistsException {
		int colPos = 0; // Current position in column array
		int dataPos = 0; // Current position in data array
		int colNum; // Number of columns
		int rowNum; // Number of rows
		int type; // Number representing Column types
		int tmp; // temporary variable

		/** All possible arrays, to speedup writing */
		long[] longs = null;
		Date[] dates = null;

		// Resulting reference
		MyTable t;
		List<Table> list = new ArrayList<Table>();

		for (int i = 0; i < tableNumber; i++) {
			t = new MyTable(names[i]);
			colNum = cols[colPos++];
			rowNum = cols[colPos++];

			// Add columns
			for (int j = 0; j < colNum; j++) {
				type = cols[colPos++];
				/**
				 * double: 1; float: 2; long: 3; integer: 4; varchar: 5; date:
				 * 6; char: (length)7
				 */
				switch (type) {
				case 1:
					if (rowNum > 0)
						t.addColumn(new MyDoubleColumn(names[tableNumber
								+ dataPos], (double[]) data[dataPos++]));
					else
						t.addColumn(new MyDoubleColumn(names[tableNumber
								+ dataPos++]));
					break;

				case 2:
					if (rowNum > 0)
						t.addColumn(new MyFloatColumn(names[tableNumber
								+ dataPos], (float[]) data[dataPos++]));
					else
						t.addColumn(new MyFloatColumn(names[tableNumber
								+ dataPos++]));
					break;

				case 3:
					if (rowNum > 0)
						t.addColumn(new MyLongColumn(names[tableNumber
								+ dataPos], (long[]) data[dataPos++]));
					else
						t.addColumn(new MyLongColumn(names[tableNumber
								+ dataPos++]));
					break;

				case 4:
					if (rowNum > 0)
						t.addColumn(new MyIntColumn(
								names[tableNumber + dataPos],
								(int[]) data[dataPos++]));
					else
						t.addColumn(new MyIntColumn(names[tableNumber
								+ dataPos++]));
					break;

				case 5:
					if (rowNum > 0)
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos], Types.getVarcharType(),
								(String[]) data[dataPos++]));
					else
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos++], Types.getVarcharType()));
					break;

				case 6:
					if (rowNum > 0) {
						longs = (long[]) data[dataPos];
						tmp = longs.length;
						dates = new Date[tmp];
						for (int k = 0, n = longs.length; k < n; k++) {
							if (longs[k] > 0)
								dates[k] = new Date(longs[k]);
						}
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos++], Types.getDateType(), dates));
					} else
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos++], Types.getDateType()));
					break;

				default: // char type
					if (type < 7 || type % 10 != 7)
						throw new IOException();
					else if (rowNum > 0)
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos], Types.getCharType(type / 10),
								(String[]) data[dataPos++]));
					else
						t.addColumn(new MyObjectColumn(names[tableNumber
								+ dataPos++], Types.getCharType(type / 10)));
					break;
				}
			}

			// Add rows - NOTE: This is MyRow, not SampleRow
			for (int j = 0; j < rowNum; j++)
				t.addNewRow(new MyRow(t, j));

			list.add(t);
		}
		return list;
	}

	public String[] getNames() {
		return names;
	}

	public void setNames(String[] names) {
		this.names = names;
	}

	public int getTableNumber() {
		return tableNumber;
	}

	public void setTableNumber(int tableNumber) {
		this.tableNumber = tableNumber;
	}

	public int[] getCols() {
		return cols;
	}

	public void setCols(int[] cols) {
		this.cols = cols;
	}

	public Serializable[] getData() {
		return data;
	}

	public void setData(Serializable[] data) {
		this.data = data;
	}

	public boolean initialize(Operator<? extends Table> table) {
		// Pass 1, retrieve: the number of tables, the total number of cols
		int tblNum = 0, colNum = 0;

		Table t;
		table.open();

		while ((t = table.next()) != null) {
			tblNum++;
			colNum += ((MyTable) t).getColumnCount();
		}

		if (tblNum == 0)
			return false;

		// Pass 2, fill the actual data
		names = new String[tblNum + colNum];
		data = new Serializable[colNum];
		tableNumber = tblNum;
		cols = new int[colNum + tblNum * 2];

		table.open();

		int tableNo = 0, colPos = 0, dataPos = 0, size = 0;
		Operator<MyColumn> columns;
		MyColumn c;
		Type type;
		Object obj;
		Object[] objs;
		long[] longs;

		while ((t = table.next()) != null) {
			names[tableNo++] = t.getTableName();
			cols[colPos++] = ((MyTable) t).getColumnCount();
			cols[colPos++] = size = ((MyTable) t).getActualRowCount();

			columns = ((MyTable) t).getAllColumns();

			columns.open();

			while ((c = columns.next()) != null) {
				names[dataPos + tblNum] = c.getColumnName();
				type = c.getColumnType();

				if (type != Types.getDateType()) {
					if (type == Types.getDoubleType())
						cols[colPos++] = 1;
					else if (type == Types.getFloatType())
						cols[colPos++] = 2;
					else if (type == Types.getLongType())
						cols[colPos++] = 3;
					else if (type == Types.getIntegerType())
						cols[colPos++] = 4;
					else if (type == Types.getVarcharType())
						cols[colPos++] = 5;
					else
						cols[colPos++] = type.getLength() * 10 + 7;

					obj = (c).getActualDataArrayAsObject();
					data[dataPos++] = (size > 0) ? (Serializable) obj
							: MyNull.NULLOBJ;
				} else {
					cols[colPos++] = 6;
					obj = (c).getActualDataArrayAsObject();
					if (size == 0)
						data[dataPos++] = MyNull.NULLOBJ;
					else {
						longs = new long[size];
						objs = (Object[]) obj;
						for (int i = 0; i < size; i++)
							longs[i] = ((Date) objs[i]).getTime();
						data[dataPos++] = longs;
					}
				}
			}
		}
		return true;
	}
}
