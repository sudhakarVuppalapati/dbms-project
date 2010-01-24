package metadata;

/**
 * Class to supply types
 * 
 * 
 */
public class Types {

	private enum FixedType implements Type {
		FLOAT(java.lang.Float.class, -1, "Float"),
		DOUBLE(java.lang.Double.class, -1, "Double"), 
		INTEGER(java.lang.Integer.class, -1, "Integer"), 
		LONG(java.lang.Long.class, -1, "Long"), 
		VARCHAR(java.lang.String.class, -1, "Varchar"),
		DATE(java.util.Date.class, -1, "Date");

		private FixedType(Class<?> clazz, int length, String name) {
			this.clazz = clazz;
			this.length = length;
			this.name = name;
		}

		private final Class<?> clazz;

		private final int length;

		private final String name;

		public Class<?> getCorrespondingJavaClass() {
			return clazz;
		}

		public String getName() {
			return name;
		}

		public int getLength() {
			return length;
		}

	}

	/**
	 * @param length
	 *            length of CHAR
	 * @return CHAR type
	 */
	public static final Type getCharType(int length) {
		return new CharType(length);
	}

	/**
	 * @return integer type
	 */
	public static final Type getIntegerType() {
		return FixedType.INTEGER;
	}

	/**
	 * @return VARCHAR type
	 */
	public static final Type getVarcharType() {
		return FixedType.VARCHAR;
	}

	/**
	 * @return Long type
	 */
	public static final Type getLongType() {
		return FixedType.LONG;
	}

	/**
	 * @return Double type
	 */
	public static final Type getDoubleType() {
		return FixedType.DOUBLE;
	}

	/**
	 * @return Date type
	 */
	public static final Type getDateType() {
		return FixedType.DATE;
	}

	/**
	 * @return Float type
	 */
	public static final Type getFloatType() {
		return FixedType.FLOAT;
	}

	/*
	 * Special for char as variable size
	 */
	static final class CharType implements Type {

		public CharType(int length) {
			this.length = length;
			this.name = "Char(" + length + ")";
		}

		private final int length;

		private final String name;

		public int getLength() {
			return length;
		}

		public String getName() {
			return name;
		}

		public Class<?> getCorrespondingJavaClass() {
			return java.lang.String.class;
		}

		@Override
		public boolean equals(Object obj) {
			return (obj instanceof CharType && ((CharType) obj).length == this.length);
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}

	}

}