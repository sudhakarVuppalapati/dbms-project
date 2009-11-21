package firstmilestone;

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
	protected static Map<String, Type> generateRandSchema(Random rand) {

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
	protected static java.util.Date getRandDate(Random rand) {

		long tStart = 0;
		long tEnd = (System.currentTimeMillis());

		java.util.Date date = new java.util.Date(
				(long) (rand.nextDouble() * (tEnd - tStart)) + tStart);
		Helpers.print(date.toString(), Consts.printType.INFO);
		return date;

	}

	public static ArrayList<Object> getRandDateList(Random rand,
			int tableCardinatlity) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinatlity; i++) {

			randList.add(getRandDate(rand));
		}

		return randList;
	}

	public static ArrayList<Object> getRandDoubleList(Random rand,
			int tableCardinality) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			java.lang.Double D = new java.lang.Double(rand.nextDouble());
			randList.add(D);
			Helpers.print(D.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	public static ArrayList<Object> getRandFloatList(Random rand,
			int tableCardinality) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			java.lang.Float F = new java.lang.Float(rand.nextFloat());
			randList.add(F);
			Helpers.print(F.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	protected static ArrayList<Object> getRandIntegerList(Random rand,
			int tableCardinality) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			java.lang.Integer I = new java.lang.Integer(rand.nextInt());
			randList.add(I);
			Helpers.print(I.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	public static ArrayList<Object> getRandLongList(Random rand,
			int tableCardinality) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			java.lang.Long L = new java.lang.Long(rand.nextLong());
			randList.add(L);
			Helpers.print(L.toString(), Consts.printType.INFO);

		}

		return randList;
	}

	public static ArrayList<Object> getRandVarcharList(Random rand,
			int tableCardinality) {

		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			int length = rand.nextInt(Consts.maxVarcharLength
					- Consts.minVarcharLength + 1)
					+ Consts.minVarcharLength;

			randList.add(getRandString(rand, length));

		}

		return randList;
	}

	public static ArrayList<Object> getRandCharList(Random rand,
			int tableCardinality, int maxLength) {
		ArrayList<Object> randList = new ArrayList<Object>();

		for (int i = 0; i < tableCardinality; i++) {

			int length = rand.nextInt(maxLength - Consts.minCharLength + 1)
					+ Consts.minCharLength;

			randList.add(getRandString(rand, length));

		}

		return randList;

	}

	protected static String getRandString(Random rand, int length) {

		StringBuffer sb = new StringBuffer(length);

		for (int i = 0; i < length; i++) {

			int offset = rand.nextInt(Consts.chars.length);
			sb.append(Consts.chars[offset]);
		}

		Helpers.print(sb.toString(), Consts.printType.INFO);
		return sb.toString();

	}
}
