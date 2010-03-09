package myDB;

import java.io.Serializable;

public class MyNull implements Comparable, Serializable {

	public static final MyNull NULLOBJ = new MyNull();

	private MyNull() {
	}

	@Override
	public int compareTo(Object o) {
		return -1;
	}

	@Override
	public String toString() {
		return "dummy object";
	}
	
	@Override
	public int hashCode() {
		return 0;
	}
}
