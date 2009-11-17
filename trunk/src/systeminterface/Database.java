package systeminterface;

import java.io.IOException;

import myDB.MyStorageLayer;

/**
 * The main interface to the database system. Through this class you can access
 * the different components of the database.
 * 
 * 
 */
public final class Database {

	/**
	 * To retrieve static singleton
	 * 
	 * @return database instance
	 */
	public static final Database getInstance() {
		return singleton;
	}

	private final StorageLayer storageLayer;
	private final static Database singleton = new Database();

	/************************************************************************************************
	 * Static methods instance is singleton
	 */
	/**
	 * Constructor for Database, initializes the storage layer
	 * 
	 */
	private Database() {
		this.storageLayer = new MyStorageLayer();
	}

	/****************************************************************************************
	 * Non-static methods
	 * 
	 * @return storage layer of DB
	 */

	/**
	 * 
	 * Stop this database instance and write tables back to disk
	 * 
	 */
	public void shutdownSystem() {
		try {
			storageLayer.writeTablesFromMainMemoryBackToExtent(this
					.getStorageInterface().getTables());
		} catch (IOException ex) {
		}
	}

	/**
	 * Start this database instance and get any tables that were previously
	 * stored on disk back to main memory
	 */
	public void startSystem() {
		try {
			storageLayer.loadTablesFromExtentIntoMainMemory();
		} catch (IOException ex) {
		}
	}

	/**
	 * Gives access to the singleton storage layer instance of the database
	 * 
	 * @return reference to the storage interface
	 */
	public StorageLayer getStorageInterface() {
		return storageLayer;
	}

}
