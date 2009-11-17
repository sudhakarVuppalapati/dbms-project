package operator;

/**
 * Abstract Operator
 * 
 * @author joerg
 * 
 * @param <OUTPUT>
 *            Output type of operator
 */
public abstract class AbstractOperator<OUTPUT> implements Operator<OUTPUT> {

	@Override
	public void close() {
	}

	@Override
	public void open() {
	}

}
