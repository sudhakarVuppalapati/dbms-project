package myDB;

import util.ComparisonOperator;
import metadata.Type;
import metadata.Types;
import exceptions.NoSuchRowException;
import exceptions.SchemaMismatchException;

/**
 * @author razvan, attran
 */
public class MyIntColumn extends MyColumn {

	private int[] data;
	private int curSize;

	public MyIntColumn(String name) {
		super(name);
		data = new int[INIT_CAPACITY];
	}

	public MyIntColumn(String name, int initialCapacity) {
		super(name);
		if (initialCapacity > 0)
			data = new int[Math.round(initialCapacity * FACTOR)];
		else
			data = new int[INIT_CAPACITY];
	}

	public MyIntColumn(String name, int[] coldata) {
		super(name);
		data = coldata;
		curSize = coldata.length;
	}

	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public void setData(Object coldata, int curColSize) {
		data = (int[]) coldata;
		curSize = curColSize;
	}

	@Override
	public Integer getElement(int rowID) throws NoSuchRowException {
		if (rowID >= curSize)
			throw new NoSuchRowException();

		if (data[rowID] == Integer.MIN_VALUE)
			return null;

		return new Integer(data[rowID]);
	}

	@Override
	public int getRowCount() {
		return curSize;
	}

	@Override
	public void add(Object newData) {
		// check if there is place for a new value
		if (curSize == data.length) {
			// if not, allocate a new array
			int[] data1 = new int[Math.round(FACTOR * curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data = data1;
			data1 = null; // try to force garbage collection
		}

		// add the new value
		data[curSize] = ((Integer) newData).intValue();
		curSize++;
	}

	@Override
	public void remove(int rowID) {
		data[rowID] = Integer.MAX_VALUE;
	}

	@Override
	public void update(int rowID, Object value) {
		if (value == null)
			data[rowID] = Integer.MIN_VALUE;
		else
			data[rowID] = ((Integer) value).intValue();
	}

	@Override
	public void eraseOldArray() {
		data = null;
		curSize = 0;
	}

	@Override
	protected Object getActualDataArrayAsObject() {
		// Pass 1: Count the un-deleted rows
		int cnt = 0;
		for (int i = 0; i < curSize; i++)
			if (data[i] != Integer.MAX_VALUE)
				cnt++;

		// Pass 2: Construct the new, truncated result
		int[] result = new int[cnt];
		int j = 0;
		for (int i = 0; i < curSize; i++)
			if (data[i] != Integer.MAX_VALUE)
				result[j++] = data[i];
		return result;
	}

	@Override
	public Type getColumnType() {
		return Types.getIntegerType();
	}

	// NOTE: We didn't check the deleted value. That should be done in higher
	// layer
	protected static final boolean compare(Object obj1, Object obj2,
			ComparisonOperator op) throws SchemaMismatchException {

		Integer i1 = (Integer) obj1;
		Integer i2 = (Integer) obj2;

		if (op == ComparisonOperator.EQ)
			if (i1.compareTo(i2) == 0)
				return true;
			else
				return false;

		if (op == ComparisonOperator.NEQ)
			if (i1.compareTo(i2) != 0)
				return true;
			else
				return false;

		// If one of the values are null, then should return false;
		else if (i1 == Integer.valueOf(Integer.MIN_VALUE)
				|| i2 == Integer.valueOf(Integer.MIN_VALUE))
			return false;

		else {
			if (op == ComparisonOperator.GEQ)
				if (i1.compareTo(i2) >= 0)
					return true;
				else
					return false;

			if (op == ComparisonOperator.LEQ)
				if (i1.compareTo(i2) <= 0)
					return true;
				else
					return false;

			if (op == ComparisonOperator.GT)
				if (i1.compareTo(i2) > 0)
					return true;
				else
					return false;

			if (op == ComparisonOperator.LT)
				if (i1.compareTo(i2) < 0)
					return true;
				else
					return false;

			else
				throw new SchemaMismatchException();
		}
	}
}
