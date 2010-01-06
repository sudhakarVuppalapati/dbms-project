package myDB;

public class MyNull implements Comparable {
	public static MyNull NULLOBJ;

	@Override
	public int compareTo(Object o) {
		return -1;
	}
}
