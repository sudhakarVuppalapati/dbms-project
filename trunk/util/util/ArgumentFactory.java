package util;

/**
 * 
 * A factory accepting arguments
 * 
 * @author myahya
 * 
 * @param <E>
 * @param <ARGUMENT>
 */
public interface ArgumentFactory<E, ARGUMENT> {
	/**
	 * @param arg
	 * @return new instance of E
	 */
	public E newInstance(ARGUMENT arg);
}
