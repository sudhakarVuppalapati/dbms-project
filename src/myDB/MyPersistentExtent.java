/**
 * 
 */
package myDB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import systeminterface.PersistentExtent;

/**
 * @author tuanta
 *
 */
public class MyPersistentExtent implements PersistentExtent {

	public static final String TABLES_METADATA_FILE = "disk/dbtables.dat";
	
	public static final int BUFF_SIZE = 1048576;
	
	public static final byte[] buff = new byte[BUFF_SIZE];
	
	@Override
	public int appendData(byte[] data) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void assignStorageSpace(File file, long startOffsetIncluding,
			long endOffsetExcluding) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropData(int ID) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] getData(int ID) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void updateData(int ID, byte[] newData) throws IOException {
		// TODO Auto-generated method stub
		
	}

}