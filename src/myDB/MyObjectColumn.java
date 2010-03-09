package myDB;

import java.util.Date;

import util.ComparisonOperator;
import metadata.Type;
import metadata.Types;
import exceptions.NoSuchRowException;
import exceptions.SchemaMismatchException;

/**
 * @author razvan, attran
 */
public class MyObjectColumn extends MyColumn {

	private Object[] data;
	int curSize;
	private Type type;

	public MyObjectColumn(String name, Type coltype) {
		super(name);
		type = coltype;
		data = new Object[INIT_CAPACITY];
	}

	public MyObjectColumn(String name, Type coltype, int initialCapacity) {
		super(name);
		type = coltype;
		if (initialCapacity > 0)
			data = new Object[Math.round(initialCapacity * FACTOR)];
		else
			data = new Object[INIT_CAPACITY];
	}

	public MyObjectColumn(String name, Type coltype, Object[] coldata) {
		super(name);
		type = coltype;
		data = coldata;
		curSize = coldata.length;
	}

	@Override
	public Object getDataArrayAsObject() {
		return data;
	}

	@Override
	public void setData(Object coldata, int curColSize) {
		data = (Object[]) coldata;
		curSize = curColSize;
	}

	@Override
	public Object getElement(int rowID) throws NoSuchRowException {
		if (rowID >= curSize)
			throw new NoSuchRowException();

		return data[rowID];
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
			Object[] data1 = new Object[Math.round(FACTOR * curSize)];
			System.arraycopy(data, 0, data1, 0, curSize);
			data = data1;
			data1 = null; // try to force garbage collection
		}

		// add the new value
		data[curSize] = newData;
		curSize++;
	}

	@Override
	public void remove(int rowID) {
		data[rowID] = MyNull.NULLOBJ;
	}

	@Override
	public void update(int rowID, Object value) {
		data[rowID] = value;
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
			if (data[i] != MyNull.NULLOBJ)
				cnt++;

		// Pass 2: Construct the new, truncated result
		int j = 0;
		if (type == Types.getDateType()) {
			Date[] result = new Date[cnt];
			for (int i = 0; i < curSize; i++)
				if (data[i] != MyNull.NULLOBJ)
					result[j++] = (Date) data[i];
			return result;

		} else {
			String[] result = new String[cnt];
			for (int i = 0; i < curSize; i++)
				if (data[i] != MyNull.NULLOBJ)
					result[j++] = (String) data[i];
			return result;
		}
	}

	@Override
	public Type getColumnType() {
		return type;
	}

	// NOTE: We didn't check the deleted value. That should be done in higher
	// layer
	protected static final boolean compare(Object obj1, Object obj2,
			ComparisonOperator op, Type type) throws SchemaMismatchException {
		Comparable i1, i2;

		if (type == Types.getDateType()) {
			i1 = (Date) obj1;
			i2 = (Date) obj2;
		} else {
			i1 = (String) obj1;
			i2 = (String) obj2;
		}

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
		else if (i1 == MyNull.NULLOBJ || i2 == MyNull.NULLOBJ)
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
