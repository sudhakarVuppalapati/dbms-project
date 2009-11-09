package systeminterface;

import sampleDB.SampleStorageLayer;

/**
 * The main interface to the database system. Through this class you can access
 * the different components of the database.
 * 
 * @author myahya
 */
final public class Database {

	/**
	 * To retrieve static singleton
	 * 
	 * @return database instance
	 */
	public static Database getInstance() {
		return singleton;
	}

	private final StorageLayer storageLayer;

	private static Database singleton;

	/************************************************************************************************
	 * Static methods instance is singleton
	 */

	/**
	 * Initializes Database
	 */
	public static void initDatabase() {

		/*
		 * TODO make sure only one is created?
		 */
		singleton = new Database();
	}

	/**
	 * Constructor for Database, initializes the storage layer
	 * 
	 */
	private Database() {
		this.storageLayer = new SampleStorageLayer();
	}

	/****************************************************************************************
	 * Non-static methods
	 */

	/**
	 * Gives access to the singleton storage layer instance of the database
	 * 
	 * @return reference to the storage interface
	 */
	public StorageLayer getStorageInterface() {
		return storageLayer;
	}

	/**
	 * Stop this database instance and write tables back to disk
	 */
	public void shutdownSystem() {
		storageLayer.writeTablesFromMainMemoryBackToExtent();
	}

	/**
	 * Start this database instance and get any tables that were previously
	 * stored on disk back to main memory
	 */
	public void startSystem() {
		storageLayer.loadTablesFromExtentIntoMainMemory();
	}

}
