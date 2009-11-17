package systeminterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * All access to external storage has to be wrapped by an instance of this
 * interface
 * 
 */
public interface PersistentExtent {

	/**
	 * @param data
	 *            Byte array with data to write
	 * @return A unique identifier for the data you appended to the extent
	 *         allowing you to retrieve it later
	 * @throws IOException
	 *             Where append fails due to IO
	 */
	public int appendData(byte[] data) throws IOException;

	/**
	 * 
	 * Assign storage space for an extent within a file
	 * 
	 * @param file
	 *            File where extent is located
	 * @param startOffsetIncluding
	 *            Start offset within file
	 * @param endOffsetExcluding
	 *            End offset within file
	 * @throws FileNotFoundException
	 *             FileNotFoundException
	 * @throws IOException
	 *             IOException
	 */
	public void assignStorageSpace(File file, long startOffsetIncluding,
			long endOffsetExcluding) throws FileNotFoundException, IOException;

	/**
	 * 
	 * Drop part of the data in the extent
	 * 
	 * @param ID
	 *            An identifier previously returned by appendData
	 * @throws IOException
	 *             Where drop data fails due to IO
	 */
	public void dropData(int ID) throws IOException;;

	/**
	 * 
	 * Get the data in the extent
	 * 
	 * @param ID
	 *            Unique identifier of data block within extent
	 * @return A byte array with the requested data
	 * @throws IOException
	 *             Failure due to IO
	 */
	public byte[] getData(int ID) throws IOException;;

	/**
	 * @return The size of the extent
	 */
	public long size();

	/**
	 * 
	 * Update the data in the block identified by ID
	 * 
	 * @param ID
	 *            Block ID
	 * @param newData
	 *            Byte array with new data
	 * @throws IOException
	 *             Failure due to IO
	 */
	public void updateData(int ID, byte[] newData) throws IOException;

}
