/**
 * 
 */
package myDB.btree.util;

/**
 * @author attran
 *
 */
public class MyIntPushOperator implements IntPushOperator {

	private static final int INTIAL_CAPACITY = 20;
	
	private static final float LOAD_FACTOR = 1.5f;
	
	private int pos;
	
	private int[] data;
	
	public MyIntPushOperator() {
		pos = 0;
		data = new int[INTIAL_CAPACITY];
	}
	@Override
	public void pass(int element) {
		if (pos >= data.length) {
			int[] newData = new int[Math.round(INTIAL_CAPACITY * LOAD_FACTOR)];
			System.arraycopy(data, 0, newData, 0, pos);
			data = newData;
			newData = null;		//force garbage collection
		}
		data[pos++] = element;
	}

	@Override
	public void thatsallfolks() {
	}
	
	public int[] getData() {
		return data;
	}

}
