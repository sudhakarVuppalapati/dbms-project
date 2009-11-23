package myDB;


import metadata.Type;
import util.Pair;

public class DataCell {
	
	private Type type;
	private Object value;

	public DataCell(Type type, Object value) {
		this.type = type;
		this.value = value;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Type getType() {
		return type;
	}
}
