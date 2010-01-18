package myDB;

import systeminterface.IndexLayer;
import systeminterface.StorageLayer;

public abstract class Processor {
	private final IndexLayer indexLayer;
	private final StorageLayer storageLayer;
	
	public Processor(IndexLayer iLayer, StorageLayer sLayer) {
		indexLayer = iLayer;
		storageLayer = sLayer;
	}
}
