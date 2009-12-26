package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import metadata.Type;
import metadata.Types;

/**
 * @author myahya
 * 
 */
public class RandomInputGenerator {
	/**
	 * Generate a random schema
	 * 
	 * @param rand
	 * @return
	 * 
	 */
	public static Map<String, Type> generateRandSchema(Random rand) {

		Types types = new Types();

		Map<String, Type> schema = new HashMap<String, Type>();
		int dimension = rand.nextInt(Consts.schemaMaxDim - Consts.schemaMinDim
				+ 1)
				+ Consts.schemaMinDim;
		Helpers.print("Dimension = " + dimension, Consts.printType.INFO);

		for (int i = 0; i < dimension; i++) {

			String colName = "col_" + i;

			Helpers.print(colName, Consts.printType.INFO);

			int columnType = rand.nextInt(7);

			Type t = null;

			switch (columnType) {

			case 0:
				int len = rand.nextInt(Consts.maxCharLength
						- Consts.minCharLength + 1)
						+ Consts.minCharLength;
				t = Types.getCharType(len);
				Helpers.print("Type: Char, length:" + len,
						Consts.printType.INFO);
				break;
			case 1:
				t = Types.getDateType();
				Helpers.print("Type: Date", Consts.printType.INFO);
				break;
			case 2:
				t = Types.getDoubleType();
				Helpers.print("Type: Double", Consts.printType.INFO);
				break;
			case 3:
				t = Types.getFloatType();
				Helpers.print("Type: Float", Consts.printType.INFO);
				break;
			case 4:
				t = Types.getIntegerType();
				Helpers.print("Type: Integer", Consts.printType.INFO);
				break;
			case 5:
				t = Types.getLongType();
				Helpers.print("Type: Long", Consts.printType.INFO);
				break;
			case 6:
				t = Types.getVarcharType();
				Helpers.print("Type: Varchar", Consts.printType.INFO);
				break;

			}

			schema.put(colName, t);

		}

		return schema;

	}

	/**
	 * 
	 * Generate random date
	 * 
	 * @param rand
	 * @return
	 */
	public static java.util.Date getRandDate(Random rand) {

		long tStart = 0;
		long tEnd = (System.currentTimeMillis());

		java.util.Date date = new java.util.Date(
				(long) (rand.nextDouble() * (tEnd - tStart)) + tStart);
		Helpers.print(date.toString(), Consts.printType.INFO);
		return date;

	}

	/**
	 * @param rand
	 * @param size
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandDateList(Random rand, int size) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			randList.add(getRandDate(rand));
		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandDoubleList(Random rand, int size) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			java.lang.Double D = new java.lang.Double(rand.nextDouble());
			randList.add(D);
			Helpers.print(D.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandFloatList(Random rand, int size) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			java.lang.Float F = new java.lang.Float(rand.nextFloat());
			randList.add(F);
			Helpers.print(F.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @return
	 */
	public static ArrayList<Object> getRandIntegerList(Random rand, int size) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			java.lang.Integer I = new java.lang.Integer(rand.nextInt());
			randList.add(I);
			Helpers.print(I.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandLongList(Random rand, int size) {

		// Random long covers Long.MIN_VALUE to Long.MAX_VALUE
		// For nulls, must ensure that Long.MIN_VALUE does not get generated

		Long minLong = new Long(java.lang.Long.MIN_VALUE);

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			java.lang.Long L;
			while ((L = new java.lang.Long(rand.nextLong())).equals(minLong)) {
				;
			}
			randList.add(L);
			Helpers.print(L.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandVarcharList(Random rand, int size) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			int length = rand.nextInt(Consts.maxVarcharLength
					- Consts.minVarcharLength + 1)
					+ Consts.minVarcharLength;

			randList.add(getRandString(rand, length));

		}

		return randList;
	}

	/**
	 * @param rand
	 * @param size
	 * @param maxLength
	 * @return return a random list
	 */
	public static ArrayList<Object> getRandCharList(Random rand, int size,
			int maxLength) {
		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < size; i++) {

			int length = rand.nextInt(maxLength - Consts.minCharLength + 1)
					+ Consts.minCharLength;

			randList.add(getRandString(rand, length));

		}

		return randList;

	}

	public static String getRandString(Random rand, int length) {

		StringBuffer sb = new StringBuffer(length);

		for (int i = 0; i < length; i++) {

			int offset = rand.nextInt(Consts.chars.length);
			sb.append(Consts.chars[offset]);
		}

		Helpers.print(sb.toString(), Consts.printType.INFO);
		return sb.toString();

	}

	/*****************************************************************/

}
