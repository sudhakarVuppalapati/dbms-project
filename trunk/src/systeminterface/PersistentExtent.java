package systeminterface;

import java.io.File;

/**
 * all access to external storage has to be wrapped by an instance of this
 * interface
 * 
 * @author jens
 */
public interface PersistentExtent {

	/**
	 * @param data
	 *            byte array with data to write
	 * @return a unique identifier for the data you appended to the extent
	 *         allowing you to retrieve it later
	 */
	public int appendData(byte[] data);

	/**
	 * @param file
	 * @param startOffsetIncluding
	 * @param endOffsetExcluding
	 */
	public void assignStorageSpace(File file, long startOffsetIncluding,
			long endOffsetExcluding);

	/**
	 * 
	 * Drop part of the data in the extent
	 * 
	 * @param ID
	 *            an identifier previously returned by appendData
	 */
	public void dropData(int ID);

	/**
	 * 
	 * Get the data in the extent
	 * 
	 * @param ID
	 * @return a byte array with the requested data
	 */
	public byte[] getData(int ID);

	/**
	 * @return the size of the extent
	 */
	public long size();

	/**
	 * 
	 * @param ID
	 * @param newData
	 */
	public void updateData(int ID, byte[] newData);

}
