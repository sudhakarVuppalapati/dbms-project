package sampleDB;

import metadata.Type;

/**
 * helper class
 * 
 */
public class ColumnInfo {

	/**
	 * Constructor
	 */
	public ColumnInfo() {

	}

	private String name;

	private Type type;

	/**
	 * @param name
	 *            Column name
	 * @param type
	 *            Column Type
	 */
	public ColumnInfo(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * @return name of column
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return type of column
	 */
	public Type getType() {
		return type;
	}

	/**
	 * @param name
	 *            name of column
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param type
	 *            type of column
	 */
	public void setType(Type type) {
		this.type = type;
	}

}
