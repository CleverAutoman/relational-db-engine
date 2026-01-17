package database.recovery;

import database.Transaction;
import database.common.Pair;
import database.concurrency.DummyLockContext;
import database.io.DiskSpaceManager;
import database.memory.BufferManager;
import database.memory.Page;
import database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // initialize a new commit record
        TransactionTableEntry entry = transactionTable.get(transNum);
        long lastLSN = entry.lastLSN;
        // add the commit record into log
        LogRecord commitTransactionLogRecord = new CommitTransactionLogRecord(transNum, lastLSN);
        logManager.appendToLog(commitTransactionLogRecord);
        // log should be flushed
        pageFlushHook(lastLSN);
        // update the transaction table & transaction status
        entry.transaction.setStatus(Transaction.Status.COMMITTING);
        return lastLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry entry = transactionTable.get(transNum);
        long lastLSN = entry.lastLSN;
        // add the commit record into log
        LogRecord AbortTransactionLogRecord = new AbortTransactionLogRecord(transNum, lastLSN);
        logManager.appendToLog(AbortTransactionLogRecord);
        // log should be flushed
//        pageFlushHook(AbortTransactionLogRecord.LSN);
        // update the transaction table & transaction status
        entry.transaction.setStatus(Transaction.Status.ABORTING);
        transactionTable.put(transNum, entry);
        return lastLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry entry = transactionTable.get(transNum);
        // should be rolled back if the txn is aborting
        // any changes need to be undone should be undone ---> after the last commit ?
        long last = 0L;
        if (entry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            last = rollbackToLSN(transNum, 0);
        }
        long lastLSN = entry.lastLSN;

        // txn removed, end record appended, txn status should be updated
        transactionTable.remove(transNum);

        entry.transaction.setStatus(Transaction.Status.COMPLETE);
        EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, last);
//        pageFlushHook(lastLSN);

        logManager.appendToLog(endTransactionLogRecord);
        return lastLSN;
    }
    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private long rollbackToLSN(long transNum, long LSN) {
        // Starting with the LSN of the most recent record that hasn't been undone:
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN(); // first record to consider
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        long lastLSN = transactionEntry.lastLSN;
        System.out.println("inititializing " + lastLSN);
        while (currentLSN > LSN) {
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                // find the
                LogRecord CLR = logRecord.undo(lastLSN); // 使用正确的LSN
                logManager.appendToLog(CLR);
                CLR.redo(this, diskSpaceManager, bufferManager);
                lastLSN = CLR.getLSN();

                System.out.println("updated + " + lastLSN);

                // 更新currentLSN到下一个需要撤销的记录
                currentLSN = CLR.getUndoNextLSN().orElse(logRecord.getPrevLSN().orElse(-1L));
            } else {
                // 如果记录不可撤销，跳到前一个记录
                currentLSN = logRecord.getPrevLSN().orElse(-1L);
            }
            if (currentLSN == -1L) break; // 没有更多记录，终止循环
        }
        return lastLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        Transaction transaction = transactionTableEntry.transaction;

        UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, transactionTableEntry.lastLSN, pageOffset, before, after);
        long lsn = logManager.appendToLog(updatePageLogRecord);
        // update the txnTable
        transactionTableEntry.lastLSN = lsn;
        // update the dpt
        dirtyPageTable.putIfAbsent(pageNum, lsn);
        return lsn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        long lastLSN = rollbackToLSN(transNum, savepointLSN);
//        LogRecord logRecord = logManager.fetchLogRecord(savepointLSN);
//        dirtyPageTable.put(logRecord.getPageNum().get(), savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        logManager.flushToLSN(beginLSN);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        List<Map.Entry<Long, Long>> dptList = new ArrayList<>();
        List<Map.Entry<Long, TransactionTableEntry>> txnList = new ArrayList<>();

        dptList.addAll(dirtyPageTable.entrySet());
        txnList.addAll(transactionTable.entrySet());

        int dptSize = dirtyPageTable.size();
        int txnSize = transactionTable.size();
        int txnTemp, dptTemp, txnBefore, dptBefore;
        txnTemp = dptTemp = txnBefore = dptBefore = 0;

        if (dptSize == 0 || txnSize == 0) return;
        LogRecord lastRecord = null;

        while (txnTemp < txnSize || dptTemp < dptSize) {
            Map<Long, Long> tempMapDPT = new HashMap<>();
            Map<Long, Pair<Transaction.Status, Long>> tempMapTxn = new HashMap<>();
            while (EndCheckpointLogRecord.fitsInOneRecord(dptTemp - dptBefore, txnTemp - txnBefore)) {
                if (dptTemp == dptSize && txnTemp == txnSize) break;
                if (dptTemp < dptSize) {
                    dptTemp++;
                } else {
                    txnTemp++;
                }
            }
            if (dptTemp != dptSize) dptTemp--;
            if (txnTemp != txnSize) txnTemp--;

            for (Map.Entry<Long, Long> entry : dptList.subList(dptBefore, dptTemp)) {
                tempMapDPT.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Long, TransactionTableEntry> entry : txnList.subList(txnBefore, txnTemp)) {
                tempMapTxn.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
            }
            lastRecord = new EndCheckpointLogRecord(tempMapDPT, tempMapTxn);
            logManager.appendToLog(lastRecord);
            dptBefore = dptTemp;
            txnBefore = txnTemp;
        }


        // Last end checkpoint record
//        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
//        logManager.appendToLog(endRecord);
//        // Ensure checkpoint is fully flushed before updating the master record
//        flushToLSN(lastRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement

        // get the trans Num
//        Optional<Long> transNumOp = record.getTransNum();
//        if (transNumOp.isEmpty()) return;
//        Long transNumber = transNumOp.get();
        // iterate from the start ckpt
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
        while (logRecordIterator.hasNext()) {
            // If the log record is for a transaction operation (getTransNum is present)
            // update the transaction table
            LogRecord next = logRecordIterator.next();
            Optional<Long> transNum = next.getTransNum();
            // check whether txn in the table and add if absent
            if (transNum.isPresent()) {
                Long num = transNum.get();
                if (!transactionTable.containsKey(num)) {
                    Transaction txn = newTransaction.apply(num);
//                    // not sure
//                    txn.setStatus(Transaction.Status.RUNNING);
                    transactionTable.put(num, new TransactionTableEntry(txn));
                    // start txn
                    startTransaction(txn);
                }
                // update the lastLSN
                transactionTable.get(num).lastLSN = next.LSN;
//                // not sure
//                transactionTable.get(num).transaction.setStatus(Transaction.Status.RUNNING);
            }
            // If the log record is page-related (getPageNum is present), update the dpt
            //     *   - update/undoupdate page will dirty pages
            //     *   - free/undoalloc page always flush changes to disk
            //     *   - no action needed for alloc/undofree page
            Optional<Long> pageNum = next.getPageNum();
            if (pageNum.isPresent()) {
                Long pageNumber = pageNum.get();
                LogType nextType = next.type;
                if (nextType.equals(LogType.UNDO_UPDATE_PAGE) || nextType.equals(LogType.UPDATE_PAGE)) {
                    dirtyPageTable.put(pageNumber, next.LSN);
                } else if (nextType.equals(LogType.UNDO_ALLOC_PAGE) || nextType.equals(LogType.FREE_PAGE)) {
                    dirtyPageTable.remove(pageNumber);
                } else {
                    // do nothing
                }
            }
            //  If the log record is for a change in transaction status:
            //     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
            //     * - update the transaction table
            //     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
            //     *   from txn table, and add to endedTransactions
            LogType type = next.type;
            if (transNum.isPresent()) {
                TransactionTableEntry entry = transactionTable.get(transNum.get());
                Transaction transaction = entry.transaction;
                if (type.equals(LogType.COMMIT_TRANSACTION)) {
                    transaction.setStatus(Transaction.Status.COMMITTING);
                    entry.lastLSN = next.LSN;
                } else if (type.equals(LogType.ABORT_TRANSACTION)) {
                    transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    entry.lastLSN = next.LSN;
                } else if (type.equals(LogType.END_TRANSACTION)) {
                    transaction.cleanup();
                    transaction.setStatus(Transaction.Status.COMPLETE);
//                    logManager.appendToLog(new EndTransactionLogRecord(transNum.get(), next.LSN));
                    // remove and add
                    transactionTable.remove(transNum.get());
                    endedTransactions.add(transNum.get());
                } else {
                    // do nothing
                }
            }
            //      * If the log record is an end_checkpoint record:
            //     * - Copy all entries of checkpoint DPT (replace existing entries if any)
            //     * - Skip txn table entries for transactions that have already ended
            //     * - Add to transaction table if not already present
            //     * - Update lastLSN to be the larger of the existing entry's (if any) and
            //     *   the checkpoint's
            //     * - The status's in the transaction table should be updated if it is possible
            //     *   to transition from the status in the table to the status in the
            //     *   checkpoint. For example, running -> aborting is a possible transition,
            //     *   but aborting -> running is not.
            if (type.equals(LogType.END_CHECKPOINT)) {
                Map<Long, Pair<Transaction.Status, Long>> nextTransactionTable = next.getTransactionTable();
                Map<Long, Long> nextDirtyPageTable = next.getDirtyPageTable();
                System.out.println("transTable size == " + nextTransactionTable.size());

                for (Map.Entry<Long, Long> entry : nextDirtyPageTable.entrySet()) {
                    Long tobeUpdated = dirtyPageTable.get(entry.getKey());
                    dirtyPageTable.put(entry.getKey(), Math.min(tobeUpdated, entry.getValue()));
                }
                // dpt debug
                for (Map.Entry<Long, Long> entry : nextDirtyPageTable.entrySet()) {
                    System.out.println("dpt debug " + entry.getKey() + " " + entry.getValue());
                }

                for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
                    System.out.println("trans debug " + entry.getKey() + " " + entry.getValue().transaction.getStatus());
                }

                System.out.println("dptable size == " + nextDirtyPageTable.size());
                if (transNum.isPresent() && endedTransactions.contains(transNum.get())) {
                    continue;
                }
//                Map<Long, TransactionTableEntry> ckptTxn = transactionTable;

                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : nextTransactionTable.entrySet()) {
                    System.out.println( "new value is " + entry.getValue().getFirst());
                    Transaction.Status status = entry.getValue().getFirst();
//                    System.out.println("new status " + status);
                    Long newLSN = entry.getValue().getSecond();
                    Transaction transaction = newTransaction.apply(entry.getKey());
                    // entry of before
                    TransactionTableEntry entryBefore = transactionTable.get(entry.getKey());
                    if (entryBefore == null) {
                        if (status.equals(Transaction.Status.ABORTING)) status = Transaction.Status.RECOVERY_ABORTING;
                        transaction.setStatus(status);
                        TransactionTableEntry transactionTableEntry = new TransactionTableEntry(transaction);
                        transactionTableEntry.lastLSN = newLSN;
                        transactionTable.put(entry.getKey(), transactionTableEntry);
                    } else {
                        Transaction transBefore = entryBefore.transaction;
                        System.out.println( "before  " + transBefore.getStatus() + " after " + entry.getValue().getFirst());
                        // debug part

                        if (transBefore.getStatus().equals(Transaction.Status.RUNNING) && status.equals(Transaction.Status.COMMITTING)) {
                            transBefore.setStatus(Transaction.Status.COMMITTING);
                        } else if (transBefore.getStatus().equals(Transaction.Status.RUNNING) && status.equals(Transaction.Status.ABORTING)) {
                            transBefore.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else if (transBefore.getStatus().equals(Transaction.Status.COMMITTING) && status.equals(Transaction.Status.COMPLETE)) {
                            transBefore.setStatus(Transaction.Status.COMPLETE);
                        } else if (transBefore.getStatus().equals(Transaction.Status.ABORTING) && status.equals(Transaction.Status.COMPLETE)) {
                            transBefore.setStatus(Transaction.Status.COMPLETE);
                        } else {
                            // do nothing
//                            transaction.setStatus(newStatus);
                        }
//                    transaction.setStatus(status);
                        TransactionTableEntry transactionTableEntry = new TransactionTableEntry(transBefore);
                        transactionTableEntry.lastLSN = Math.max(newLSN, entryBefore.lastLSN);
                        transactionTable.put(entry.getKey(), transactionTableEntry);
                    }

                }
                for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
                    System.out.println("trans after updating " + entry.getKey() + " " + entry.getValue().transaction.getStatus());
                }
//                for (Map.Entry<Long, TransactionTableEntry> entry : ckptTxn.entrySet()) {
//                    Long tempTrans = entry.getKey();
//                    Transaction trans = entry.getValue().transaction;
//                    Transaction ckptTransaction = ckptTxn.get(tempTrans).transaction;
//
//                }
            }
            for (Long l : endedTransactions) {
                transactionTable.remove(l);
            }
        }
        //     * After all records in the log are processed, for each ttable entry:
        //     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
        //     *    remove from the ttable, and append an end record
        //     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
        //     *    record
        //     *  - if RECOVERY_ABORTING: no action needed
//        System.out.println(transactionTable.size());
//        transactionTable.entrySet().removeIf(entry -> entry.getValue().transaction.getStatus().equals(Transaction.Status.COMPLETE));
        System.out.println("after " + transactionTable.size());

        for (TransactionTableEntry value : transactionTable.values()) {
            long transNum = value.transaction.getTransNum();
            System.out.println(value.transaction.getStatus());
            if (value.transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                value.transaction.cleanup();
                value.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
                logManager.appendToLog(new EndTransactionLogRecord(transNum, value.lastLSN));
            } else if (value.transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                value.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                long l = logManager.appendToLog(new AbortTransactionLogRecord(transNum, value.lastLSN));
                value.lastLSN = l;
            }
        }

        System.out.println(transactionTable.size());
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue().transaction.getStatus());
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        Long smallestRecID = Long.MAX_VALUE;
        Map.Entry<Long, Long> smallestEntry = null;
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            if (entry.getValue() < smallestRecID) {
                smallestRecID = entry.getValue();
                smallestEntry = entry;
            }
        }
        if (smallestEntry == null) return;
        Long pageNum = smallestEntry.getKey();
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(smallestRecID);
        while (logRecordIterator.hasNext()) {
            LogRecord next = logRecordIterator.next();

            if (next.isRedoable()) {
                if (next.type.equals(LogType.ALLOC_PART) || next.type.equals(LogType.UNDO_ALLOC_PART)
                        || next.type.equals(LogType.FREE_PART) || next.type.equals(LogType.UNDO_FREE_PART)) {
                    next.redo(this, diskSpaceManager, bufferManager);
                } else if (next.type.equals(LogType.ALLOC_PAGE) || next.type.equals(LogType.UNDO_FREE_PAGE)) {
                    next.redo(this, diskSpaceManager, bufferManager);
                } else if (next.type.equals(LogType.UPDATE_PAGE) || next.type.equals(LogType.UNDO_UPDATE_PAGE)
                        || next.type.equals(LogType.UNDO_ALLOC_PAGE) || next.type.equals(LogType.FREE_PAGE)) {
                    long lsn = next.getLSN();
                    Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                    try {
                        // Do anything that requires the page here
                        long pageLSN = page.getPageLSN();
                        if (dirtyPageTable.containsKey(pageNum) || lsn >= smallestRecID
                            || pageLSN < lsn) {
                            next.redo(this, diskSpaceManager, bufferManager);
                        }
                    } finally {
                        page.unpin();
                    }
                }
            }
        }

        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<TransactionTableEntry> priorityQueue = new PriorityQueue<>((a, b) -> Long.compare(b.lastLSN, a.lastLSN));
        for (TransactionTableEntry entry : transactionTable.values()) {
            if (entry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                priorityQueue.add(entry);
            }
        }

        while (!priorityQueue.isEmpty()) {
            TransactionTableEntry currentEntry = priorityQueue.poll();
            long lsn = currentEntry.lastLSN;
            LogRecord currentLogRecord = logManager.fetchLogRecord(lsn);

            if (currentLogRecord.isUndoable()) {
                LogRecord clr = currentLogRecord.undo(lsn);
                long clrLSN = logManager.appendToLog(clr);  // Append CLR to the log and get its LSN
                currentEntry.lastLSN = clrLSN;  // Update the LSN in the entry
                clr.redo(this, diskSpaceManager, bufferManager);  // Perform the actual undo operation via redo

                if (clr.getPageNum().isPresent()) {
                    dirtyPageTable.put(clr.getPageNum().get(), clrLSN);
                }
            }

            // Determine the next LSN to process for the current transaction
            Long nextLSN = currentLogRecord.getUndoNextLSN().orElse(currentLogRecord.getPrevLSN().orElse(0L));
            if (nextLSN == 0) {
                currentEntry.transaction.cleanup();  // Clean up transaction resources
                currentEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(currentEntry.transaction.getTransNum());  // Remove from transaction table
            } else {
                currentEntry.lastLSN = nextLSN;  // Update the entry with the next LSN
                priorityQueue.add(currentEntry);  // Re-add the entry to the queue for further processing
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
