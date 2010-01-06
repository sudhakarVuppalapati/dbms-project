package systeminterface;

import java.io.IOException;

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

	private final IndexLayer indexLayer;

	private final QueryLayer queryLayer;

	private final static Database singleton = new Database();

	/***************************************************************************
	 * Static methods instance is singleton
	 */
	/**
	 * Constructor for Database, initializes the storage layer
	 * 
	 */
	private Database() {

		this.storageLayer = new myDB.MyStorageLayer();
		this.indexLayer = new myDB.MyIndexLayer(this.storageLayer);
		this.queryLayer = new myDB.MyQueryLayer(this.storageLayer,
				this.indexLayer);

	}

	/***************************************************************************
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
			//indexLayer.storeIndexInformation();
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
			//indexLayer.rebuildAllIndexes();
		} catch (IOException ex) {
		}
	}

	/**
	 * Gives access to the singleton storage layer instance of the database
	 * 
	 * @return reference to the storage interface.
	 */
	public StorageLayer getStorageInterface() {
		return storageLayer;
	}

	/**
	 * Gives access to the singleton index layer instance of the database.
	 * 
	 * @return reference to the index interface.
	 */
	public IndexLayer getIndexInterface() {
		return indexLayer;
	}

	/**
	 * Gives access to the singleton query layer instance of the database.
	 * 
	 * @return reference to the query interface.
	 */
	public QueryLayer getQueryInterface() {
		return this.queryLayer;
	}

}
