package exceptions;

/**
 * 
 */
public class ColumnAlreadyExistsException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public ColumnAlreadyExistsException() {
		super();
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public ColumnAlreadyExistsException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 */
	public ColumnAlreadyExistsException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public ColumnAlreadyExistsException(Throwable arg0) {
		super(arg0);
	}

}
