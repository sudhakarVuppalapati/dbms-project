package firstmilestone;

/**
 * @author myahya
 * 
 */
public class Helpers {
	/**
	 * printing
	 * 
	 * @param msg
	 * @param ptype
	 */
	protected static void print(String msg, Consts.printType ptype) {

		if ((ptype == Consts.printType.INFO && Consts.infoMsg)) {
			System.out.println(msg);
		}

		if ((ptype == Consts.printType.WARN && Consts.warnMsg)
				|| (ptype == Consts.printType.ERROR && Consts.errorMsg)) {
			System.err.println(msg);
		}

	}
}
