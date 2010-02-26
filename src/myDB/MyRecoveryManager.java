package myDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import exceptions.NoSuchRowException;
import exceptions.NoSuchTableException;
import exceptions.SchemaMismatchException;

import operator.Operator;
import util.LoggedOperation;

import logrecords.DeleteLogPayload;
import logrecords.LogPayload;
import logrecords.LogRecord;
import logrecords.UpdateLogPayload;

import systeminterface.LogStore;
import systeminterface.RecoveryManager;
import systeminterface.StorageLayer;

/**
 * Recovery manager. Implement this skeleton. You have to use the constructor
 * shown here.
 * 
 */
public class MyRecoveryManager implements RecoveryManager {

	private final StorageLayer storageLayer;
	
	
	/**
	 * transactions table
	 * Maps a TID to the lists of LogRecords corresponding to that transaction
	 */
	private HashMap<Long, ArrayList<LogRecord>> tt = new HashMap<Long, ArrayList<LogRecord>>();
	
	/*
	 * a linked list to keep the order of the operations
	 * this might be somehow redundant but for the moment is
	 * a decent solution
	 */
	private LinkedList<LogRecord> orderedLogs=new LinkedList<LogRecord>();

	/**
	 * 
	 * Please do not modify this constructor
	 * 
	 * @param storageLayer
	 *            A reference to the underlying storage layer
	 */
	public MyRecoveryManager(StorageLayer storageLayer) {
		this.storageLayer = storageLayer;

	}
	/**
	 * Basic strategy: 
	 * 	- Analysis phase
	 * 	- Undo phase
	 * 
	 *  This strategy is not the one used by ARIES, but a 
	 *  much simpler one. Basically, I go through the LogStore
	 *  and build the tt using the Logrecords I encounter.
	 *  
	 *  When finding an Abort/Commit LogRecord I remove the
	 *  corresponding TID from tt as it means that this transaction
	 *  completed(successfully or not) and it left the database
	 *  in a consistent state
	 *  
	 *  When reaching the end of the LogStore, the Analysis phase is
	 *  over. Now tt should contain only the unfinished transactions
	 *  that need to be undone. So, I iterate through the linkedlist 
	 *  and I issue the inverse operation of each component of the retrieved list. 
	 */
	@Override
	public void recover(LogStore logStore){
		
		System.out.println("In recover ");
		
		// aux vars
		LogRecord lr; 
		LoggedOperation loggedOp;
		LogPayload payload;
		
		String tableName;
		int rId;
		long tid;
		
		Operator<LogRecord> logRecs = (Operator<LogRecord>) logStore.getLogRecords();
		
		if(logRecs != null){
				
			
			logRecs.open();
			
			//Analysis phase
			while((lr = logRecs.next()) != null){
				loggedOp=lr.getOperation();
				tid=lr.getTID();
				
				System.out.println(loggedOp);
				
				if( loggedOp == LoggedOperation.START_TRANSACTION )
					tt.put(tid, new  ArrayList<LogRecord>());
				
				else if (loggedOp == LoggedOperation.ABORT_TRANSACTION || loggedOp == LoggedOperation.COMMIT_TRANSACTION){
					orderedLogs.removeAll(tt.remove(lr.getTID()));
				}
				
				/*
				 * if a normal operation (update, delete, insert) is encountered, just add it
				 * to the list of ops of the transaction it belongs to 
				 */
				else {
					orderedLogs.add(lr);
					tt.get(tid).add(lr);
				}
					
			}
			
			/*
			 * Redo phase: iterate through the LinkedList and apply the 
			 * inverse of each operation
			 */
			Iterator<LogRecord> it=orderedLogs.descendingIterator();
			while(it.hasNext()){
				
				lr = it.next();
				loggedOp = lr.getOperation();
				payload = lr.getLogPayload();
				tableName=payload.getTableName();
				rId=payload.getRowID();
				
				if(loggedOp == LoggedOperation.UPDATE){
					try {
						storageLayer.getTableByName(tableName).updateRow(rId,((UpdateLogPayload)payload).getOldRow());
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}
				}
				else if(loggedOp == LoggedOperation.INSERT){
					try {
						storageLayer.getTableByName(tableName).deleteRow(rId);
					} catch (NoSuchRowException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}
				}
				else{
					try {
						storageLayer.getTableByName(tableName).addRow(((DeleteLogPayload)payload).getDeletedRow());
					} catch (SchemaMismatchException e) {
						e.printStackTrace();
					} catch (NoSuchTableException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
	}

}
