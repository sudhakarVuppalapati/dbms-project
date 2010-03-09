package myDB.btree.util;

public class Infinity implements Comparable {

	public static final Infinity MAX_VALUE = new Infinity(true);

	public static final Infinity MIN_VALUE = new Infinity(false);

	private final boolean isMax;

	private Infinity(boolean max) {
		isMax = max;
	}

	@Override
	public int compareTo(Object o) {
		try {
			Comparable c = (Comparable) o;
			if (isMax) {
				if (c == MAX_VALUE)
					return 0;
				else
					return 1;
			} else {
				if (c == MIN_VALUE)
					return 0;
				else
					return -1;
			}
		} catch (ClassCastException cce) {
			cce.printStackTrace();
			return -1;
		}
	}

}
