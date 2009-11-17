package util;

/**
 * 
 * 
 * @param <E>
 *            E
 */
public interface Factory<E> {

	/**
	 * @return new instance of E
	 */
	public E newInstance();

}
