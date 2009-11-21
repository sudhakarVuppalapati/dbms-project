package util;

/**
 * 
 * A factory accepting arguments
 * 
 * @param <E>
 *            E
 * @param <ARGUMENT>
 *            Argument
 */
public interface ArgumentFactory<E, ARGUMENT> {
	/**
	 * @param arg
	 *            argument
	 * @return new instance of E
	 */
	public E newInstance(ARGUMENT arg);
}
