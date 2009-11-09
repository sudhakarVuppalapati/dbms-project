package metadata;

/**
 * Char maps to a Java String of a fixed length.
 * 
 * Corresponds to CHAR in database types (see
 * http://dev.mysql.com/doc/refman/5.1/en/char.html)
 * 
 * @author myahya
 * 
 */
public class Char {

	private int length;

	/**
	 * @param length
	 *            specify the maximum number of characters held by a column of
	 *            this type, corresponds to CHAR(length) in SQL
	 */
	public Char(int length) {
		this.length = length;
	}

	/**
	 * @return length of the stored string
	 */
	public int getLength() {
		return length;
	}

	/**
	 * @param length
	 *            the maximum number of characters that can be placed in a
	 *            column of this type
	 */
	public void setLength(int length) {

		this.length = length;

	}

}
