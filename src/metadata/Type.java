package metadata;

/**
 * 
 * Type interface
 * 
 * 
 */
public interface Type {

	/**
	 * @return Where applicable, returns the maximum number of elements that can
	 *         be stored in this type
	 */
	public int getLength();

	/**
	 * @return A string representation of the type, should be the same as the
	 *         class name
	 */
	public String getName();

	/**
	 * @return Corresponding Java class
	 */
	public Class<?> getCorrespondingJavaClass();

}