package util;

/**
 * A pair inteface
 * 
 * @author myahya
 * 
 * @param <LEFT>
 * @param <RIGHT>
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
