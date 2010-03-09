package myDB;

import exceptions.SchemaMismatchException;
import metadata.Type;
import metadata.Types;
import util.ComparisonOperator;

/**
 * Utility class
 * 
 * @author attran
 * 
 */
public class MyUtil {

	/** Compare two values of the same */
	public static final boolean compare(Comparable obj1, Comparable obj2,
			Type type, ComparisonOperator op) throws SchemaMismatchException {

		if (type == Types.getIntegerType())
			return MyIntColumn.compare(obj1, obj2, op);

		if (type == Types.getDoubleType())
			return MyDoubleColumn.compare(obj1, obj2, op);

		if (type == Types.getLongType())
			return MyLongColumn.compare(obj1, obj2, op);

		if (type == Types.getFloatType())
			return MyFloatColumn.compare(obj1, obj2, op);

		else
			return MyObjectColumn.compare(obj1, obj2, op, type);
	}

	/**
	 * Based on Robert Jenkins' bit Mix Function, version in Java developed by
	 * Thomas Wang (http://www.concentric.net/~Ttwang/tech/inthash.htm)
	 * Copyright (c) 2007 January
	 * 
	 * @param key
	 *            the input key, as 32-bit integer
	 * @return the hash value
	 */
	public static final int hash32shiftmult(int key) {
		int c2 = 0x27d4eb2d; // a prime or an odd constant
		key = (key ^ 61) ^ (key >>> 16);
		key = key + (key << 3);
		key = key ^ (key >>> 4);
		key = key * c2;
		key = key ^ (key >>> 16);
		return key;
	}
}
