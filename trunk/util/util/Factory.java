package util;

/**
 * 
 * 
 * @author myahya
 * 
 * @param <E>
 */
public interface Factory<E> {

	/**
	 * @return new instance of E
	 */
	public E newInstance();

}
