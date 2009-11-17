package util;

/**
 * A pair interface
 * 
 * 
 * @param <LEFT>
 *            LEFT
 * @param <RIGHT>
 *            RIGHT
 */
public interface Pair<LEFT, RIGHT> {

	/**
	 * 
	 * @return left member of pair
	 */
	LEFT getLeft();

	/**
	 * @return right member of pair
	 */
	RIGHT getRight();

}
