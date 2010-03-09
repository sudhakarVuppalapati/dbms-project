package myDB.btree.util;

/**
 * This interface allows us to push integer results to a consumer. We use the
 * hard-coded primitive type for performance reasons (generics require
 * boxing/unboxing).
 * <p>
 * When a push operator is instantiated, it is assumed to be open
 * (open-on-instantiate semantics).
 * 
 * @author lukas / marcos
 */
public interface IntPushOperator {

	/**
	 * Passes the next element to the consumer.
	 * 
	 * @param element
	 */
	public void pass(int element);

	public void thatsallfolks();
}
