package myDB;

import systeminterface.IndexLayer;
import systeminterface.StorageLayer;

public abstract class Processor {
	protected final IndexLayer indexLayer;
	protected final StorageLayer storageLayer;
	
	public Processor(IndexLayer iLayer, StorageLayer sLayer) {
		indexLayer = iLayer;
		storageLayer = sLayer;
	}
}
