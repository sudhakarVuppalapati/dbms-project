package operator;

/**
 * Pull Iterator-like Interface for Operator
 * 
 * @author joerg
 * 
 * @param <OUTPUT>
 *            specifies the output type of Operator, for example row
 */
public interface Operator<OUTPUT> {

	/**
	 * close operator stream
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
	 * open operator
	 */
	void open();
}
