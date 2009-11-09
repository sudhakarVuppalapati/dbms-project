package metadata;

/**
 * 
 * Type interface
 * 
 * @author myahya
 * 
 */
public interface Type {
	/**
	 * @return where applicable, returns the maximum number of elements that can
	 *         be stored in this type
	 */
	public int getLength();

	/**
	 * @return a string representation of the type, should be the same as the
	 *         class name
	 */
	public String getName();

}
