package myDB;

import java.util.HashMap;
import exceptions.SchemaMismatchException;
import metadata.Type;
import metadata.Types;

public class Const {

	public static final int NO_NUM = 0; 
	public static final int DOUBLE_NUM = 1;
	public static final int FLOAT_NUM = 2;
	public static final int LONG_NUM = 3;
	public static final int INTEGER_NUM = 4;
	public static final int VARCHAR_NUM = 5;
	public static final int DATE_NUM = 6;
	public static final int CHAR_NUM = 7;

	public static final Type DOUBLE_TYPE  = Types.getDoubleType();
	public static final Type FLOAT_TYPE  = Types.getFloatType();
	public static final Type INTEGER_TYPE  = Types.getIntegerType();
	public static final Type LONG_TYPE  = Types.getLongType();
	public static final Type VARCHAR_TYPE  = Types.getVarcharType();
	public static final Type DATE_TYPE  = Types.getDateType();	

	private static final Type[] numMapping = {null, DOUBLE_TYPE, FLOAT_TYPE,
		FLOAT_TYPE, INTEGER_TYPE, VARCHAR_TYPE, DATE_TYPE};

	private static HashMap<Type, Integer> typeMapping = initMapping();

	private static final HashMap<Type, Integer> initMapping() {
		typeMapping = new HashMap<Type, Integer>();
		typeMapping.put(DOUBLE_TYPE, new Integer(1));
		typeMapping.put(FLOAT_TYPE, new Integer(2));
		typeMapping.put(LONG_TYPE, new Integer(3));
		typeMapping.put(INTEGER_TYPE, new Integer(4));
		typeMapping.put(VARCHAR_TYPE, new Integer(5));
		typeMapping.put(DATE_TYPE, new Integer(6));
		return typeMapping;
	}

	/**
	 * Map from type number to Type object
	 * @param typNo
	 * @return
	 */
	public static Type getType(int typNo) throws SchemaMismatchException {
		if (typNo < 7)
			return numMapping[typNo];

		if (typNo % 10 != 7)
			throw new SchemaMismatchException();

		int length = (int)typNo / 10;

		if (length == 0) 
			throw new SchemaMismatchException();

		return Types.getCharType(length);
	}


	public static int getNumber(Type type) throws SchemaMismatchException {

		if (type == null) return NO_NUM;

		try {
			return typeMapping.get(type);
		}
		catch (NullPointerException npe) {
			return (type.getLength() * 10 + CHAR_NUM);
		}
	}
}
