package myDB.physicaloperators;

import java.util.Map;
import metadata.Type;
import metadata.Types;
import myDB.MyColumn;
import myDB.MyNull;
import exceptions.NoSuchTableException;

import systeminterface.Column;

public class CrossProductOperator {

	public static Map<String, Column> product(Map<String, Column> data1,
			Map<String, Column> data2) throws NoSuchTableException {

		// Get the real cardinality of data1 and data2
		int n, size1 = 0, size2 = 0;

		Column tmpCol;
		Type type;

		if (data1 != null) {
			tmpCol = data1.values().iterator().next();
			n = tmpCol.getRowCount();
			type = tmpCol.getColumnType();

			if (type == Types.getIntegerType()) {
				int[] data = (int[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Integer.MAX_VALUE) {
						size1++;
					}
			} else if (type == Types.getFloatType()) {
				float[] data = (float[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Float.MAX_VALUE) {
						size1++;
					}
			} else if (type == Types.getDoubleType()) {
				double[] data = (double[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Double.MAX_VALUE) {
						size1++;
					}

			} else if (type == Types.getLongType()) {
				long[] data = (long[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Long.MAX_VALUE) {
						size1++;
					}
			} else {
				Object[] data = (Object[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != MyNull.NULLOBJ) {
						size1++;
					}
			}
		} else
			throw new NoSuchTableException();

		if (data2 != null) {
			tmpCol = data2.values().iterator().next();
			n = tmpCol.getRowCount();
			type = tmpCol.getColumnType();

			if (type == Types.getIntegerType()) {
				int[] data = (int[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Integer.MAX_VALUE) {
						size2++;
					}
			} else if (type == Types.getFloatType()) {
				float[] data = (float[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Float.MAX_VALUE) {
						size2++;
					}
			} else if (type == Types.getDoubleType()) {
				double[] data = (double[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Double.MAX_VALUE) {
						size2++;
					}

			} else if (type == Types.getLongType()) {
				long[] data = (long[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != Long.MAX_VALUE) {
						size2++;
					}
			} else {
				Object[] data = (Object[]) tmpCol.getDataArrayAsObject();
				for (int i = 0; i < n; i++)
					if (data[i] != MyNull.NULLOBJ) {
						size2++;
					}
			}
		} else
			throw new NoSuchTableException();

		/* List<Column> colList = new ArrayList<Column>(); */

		/**
		 * IMPORTANT NOTE: data1 is duplicated rows by rows. Data2 is duplicated
		 * blocks by blocks. Then data1 is modified and output
		 */

		// Step 1: Duplicating data1
		for (Column col : data1.values()) {
			type = col.getColumnType();
			int k = 0;

			if (type == Types.getIntegerType()) {
				int[] data = new int[size1 * size2];
				int[] cells1 = (int[]) col.getDataArrayAsObject();

				for (int i = 0, m = col.getRowCount(); i < m; i++) {
					if (cells1[i] != Integer.MAX_VALUE)
						for (int j = 0; j < size2; j++)
							data[k++] = cells1[i];
				}
				/* tmpCol = new MyIntColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getFloatType()) {
				float[] data = new float[size1 * size2];
				float[] cells1 = (float[]) col.getDataArrayAsObject();

				for (int i = 0, m = col.getRowCount(); i < m; i++) {
					if (cells1[i] != Float.MAX_VALUE)
						for (int j = 0; j < size2; j++)
							data[k++] = cells1[i];
				}
				/* tmpCol = new MyFloatColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getDoubleType()) {
				double[] data = new double[size1 * size2];
				double[] cells1 = (double[]) col.getDataArrayAsObject();

				for (int i = 0, m = col.getRowCount(); i < m; i++) {
					if (cells1[i] != Double.MAX_VALUE)
						for (int j = 0; j < size2; j++)
							data[k++] = cells1[i];
				}
				/* tmpCol = new MyDoubleColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getLongType()) {
				long[] data = new long[size1 * size2];
				long[] cells1 = (long[]) col.getDataArrayAsObject();

				for (int i = 0, m = col.getRowCount(); i < m; i++) {
					if (cells1[i] != Long.MAX_VALUE)
						for (int j = 0; j < size2; j++)
							data[k++] = cells1[i];
				}
				/* tmpCol = new MyLongColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else {
				/* List data = new ArrayList(); */
				Object[] data = new Object[size1 * size2];
				Object[] cells1 = (Object[]) col.getDataArrayAsObject();

				for (int i = 0, m = col.getRowCount(); i < m; i++) {
					if (cells1[i] != MyNull.NULLOBJ)
						for (int j = 0; j < size2; j++)
							// Reference, not copying
							// data.add(cells1[i]);
							data[k++] = cells1[i];
				}
				/* tmpCol = new MyObjectColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			}
			/* colList.add(tmpCol); */
		}

		// Step 2: Duplicating data2
		for (Column col : data2.values()) {
			type = col.getColumnType();
			int k = 0;
			if (type == Types.getIntegerType()) {
				int[] data = new int[size1 * size2];
				int[] cells2 = (int[]) col.getDataArrayAsObject();

				for (int i = 0; i < size1; i++)
					for (int j = 0, m = col.getRowCount(); j < m; j++) {
						if (cells2[j] != Integer.MAX_VALUE)
							data[k++] = cells2[j];
					}
				/* tmpCol = new MyIntColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getFloatType()) {
				float[] data = new float[size1 * size2];
				float[] cells2 = (float[]) col.getDataArrayAsObject();

				for (int i = 0; i < size1; i++)
					for (int j = 0, m = col.getRowCount(); j < m; j++) {
						if (cells2[j] != Float.MAX_VALUE)
							data[k++] = cells2[j];
					}
				/* tmpCol = new MyFloatColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getDoubleType()) {
				double[] data = new double[size1 * size2];
				double[] cells2 = (double[]) col.getDataArrayAsObject();

				for (int i = 0; i < size1; i++)
					for (int j = 0, m = col.getRowCount(); j < m; j++) {
						if (cells2[j] != Double.MAX_VALUE)
							data[k++] = cells2[j];
					}
				/* tmpCol = new MyDoubleColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else if (type == Types.getLongType()) {
				long[] data = new long[size1 * size2];
				long[] cells2 = (long[]) col.getDataArrayAsObject();

				for (int i = 0; i < size1; i++)
					for (int j = 0, m = col.getRowCount(); j < m; j++) {
						if (cells2[j] != Long.MAX_VALUE)
							data[k++] = cells2[j];
					}
				/* tmpCol = new MyLongColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			} else {
				/* List data = new ArrayList(); */
				Object[] data = new Object[size1 * size2];
				Object[] cells2 = (Object[]) col.getDataArrayAsObject();

				for (int i = 0; i < size1; i++)
					for (int j = 0, m = col.getRowCount(); j < m; j++) {
						if (cells2[j] != MyNull.NULLOBJ)
							/* data.add(cells2[j]); */
							data[k++] = cells2[j];
					}
				/* tmpCol = new MyObjectColumn(col.getColumnName(), type, data); */
				((MyColumn) col).setData(data, k);
			}
			/* colList.add(tmpCol); */
			data1.put(col.getColumnName(), col);
		}

		// Possible optmization: Creating NewHashMap is faster than clearing the
		// existing one
		// data1.clear();

		/*
		 * for (Column col : colList) { data1.put(col.getColumnName(), col); }
		 */
		return data1;
	}
}
