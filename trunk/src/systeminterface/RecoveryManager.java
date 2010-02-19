package systeminterface;

/**
 * @author myahya
 * 
 */
public interface RecoveryManager {

	/**
	 * Perform recovery
	 * 
	 * @param logStore
	 *            The log store used for recovery.
	 */

	public void recover(LogStore logStore);

}
