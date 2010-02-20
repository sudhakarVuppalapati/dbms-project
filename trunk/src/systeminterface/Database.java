package systeminterface;

import java.io.IOException;

import myDB.MyIndexLayer;
import myDB.MyQueryLayer;
import myDB.MyRecoveryManager;
import myDB.MyStorageLayer;
import myDB.MyTransactionLayer;

/**
 * The main interface to the database system. Through this class you can access
 * the different components of the database.
 * 
 */
public final class Database {

	/**
	 * To retrieve static singleton
	 * 
	 * @return database instance
	 */
	public static final Database getInstance() {

		// if database is first started or started after a crash
		if (singleton == null) {

			singleton = new Database();
		}

		return singleton;

	}

	private final StorageLayer storageLayer;

	private final IndexLayer indexLayer;

	private final QueryLayer queryLayer;

	private final TransactionLayer transactionLayer;

	private final RecoveryManager recoveryManager;

	private static Database singleton = null;

	/***************************************************************************
	 * Static methods instance is singleton
	 */
	/**
	 * Constructor for Database, initializes the storage layer
	 * 
	 */
	private Database() {

		/*
		 * this.storageLayer = new sampleDB.unreleased.SampleStorageLayer();
		 * this.indexLayer = new sampleDB.unreleased.SampleIndexLayer(
		 * this.storageLayer); this.queryLayer = new
		 * sampleDB.unreleased.SampleQueryLayer( this.storageLayer,
		 * this.indexLayer); this.transactionLayer = new
		 * sampleDB.unreleased.SampleTransactionLayer( this.storageLayer,
		 * this.indexLayer, this.queryLayer); this.recoveryManager = new
		 * sampleDB.unreleased.SampleRecoveryManager( this.storageLayer);
		 */
		this.storageLayer = new MyStorageLayer();
		this.indexLayer = new MyIndexLayer(this.storageLayer);
		this.queryLayer = new MyQueryLayer(this.storageLayer, this.indexLayer);
		this.transactionLayer = new MyTransactionLayer(this.storageLayer,
				this.indexLayer, this.queryLayer);
		this.recoveryManager = new MyRecoveryManager(this.storageLayer);

	}

	/***************************************************************************
	 * Non-static methods
	 * 
	 * @return storage layer of DB
	 */

	/**
	 * 
	 * Stop this database instance and write tables back to disk. We assume that
	 * writing to to disk is an atomic operation.
	 * 
	 */
	public void shutdownSystem() {
		try {
			storageLayer.writeTablesFromMainMemoryBackToExtent(this
					.getStorageInterface().getTables());
			indexLayer.storeIndexInformation();

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			singleton = null;
		}
	}

	/**
	 * Start this database instance and get any tables that were previously
	 * stored on disk back to main memory. This assumes that the transaction layer is
	 * not being used.
	 */
	public void startSystem() {
		try {
			storageLayer.loadTablesFromExtentIntoMainMemory();
			indexLayer.rebuildAllIndexes();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Start this database instance and get any tables that were previously
	 * stored on disk back to main memory. This assumes that the transaction
	 * layer is being used
	 * 
	 * @param logStore
	 *            The log store to use for logging and recovery
	 */
	public void startSystem(LogStore logStore) {
		try {

			this.transactionLayer.setLogStore(logStore);

			storageLayer.loadTablesFromExtentIntoMainMemory();

			recoveryManager.recover(logStore);

			indexLayer.rebuildAllIndexes();
		} catch (IOException ex) {
			ex.printStackTrace();			
		}
	}

	/**
	 * Crash the database instance
	 */
	public void crash() {
		singleton = null;
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

	/**
	 * 
	 * Gives access to the singleton transaction layer instance of the database.
	 * 
	 * @return reference to the transaction interface.
	 */
	public TransactionLayer getTransactionInterface() {
		return this.transactionLayer;

	}
}
