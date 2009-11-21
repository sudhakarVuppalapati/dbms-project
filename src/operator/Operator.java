package operator;

/**
 * Pull Iterator-like Interface for Operator
 * 
 * 
 * @param <OUTPUT>
 *            Specifies the output type of Operator, for example Row
 */
public interface Operator<OUTPUT> {

	/**
	 * Close operator stream
	 * 
	 */
	void close();

	/**
	 * 
	 * @return Output (next item), null when nothing left
	 * 
	 */
	OUTPUT next();

	/**
	 * Open operator
	 */
	void open();
}
