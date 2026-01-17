package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import javax.xml.transform.Source;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        if (lockType.equals(LockType.NL))
            throw new InvalidLockException("Attempting to acquire an NL lock, should instead use release");
        if (readonly) throw new UnsupportedOperationException("this context is readOnly");

        List<Lock> locksFromTrans = this.lockman.getLocks(transaction);
        List<Lock> locksFromSource = this.lockman.getLocks(getResourceName());
        for (Lock l : locksFromSource) {
            if (l.transactionNum.equals(transaction.getTransNum()) && l.name.equals(name))
                throw new DuplicateLockRequestException("a lock is already held by the transaction");
        }
        if (parent != null) {
            for (Lock l : locksFromTrans) {
                if (l.name == parent.name && !LockType.canBeParentLock(l.lockType, lockType)) {
                    throw new InvalidLockException("cannot be parent");
                }
            }
        }

//        System.out.println(name);

        lockman.acquire(transaction, name, lockType);
        // if lock is blocked, no adding
        LockContext pointer = this.parent;
        long transNum = transaction.getTransNum();
        while (pointer != null) {
            Integer current = pointer.numChildLocks.get(transNum);
            if (current == null)
                pointer.numChildLocks.put(transNum, 1);
            else
                pointer.numChildLocks.put(transNum, current + 1);
            pointer = pointer.parent;
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("this context is readOnly");
        List<Lock> locksFromTrans = this.lockman.getLocks(transaction);
        Lock lockToRelease = null;
        for (Lock l : locksFromTrans) {
            if (l.name == name) lockToRelease = l;
        }
//        if (lockToRelease == null)
//            throw new NoLockHeldException("no lock on `name` is held by `transaction`");
        if (lockman.getLockType(transaction, name).equals(
                LockType.NL
        )) { throw new NoLockHeldException("There is no lock on " + name.toString()); }
        // get all the children locks
        for (LockContext context : children.values()) {
            List<Lock> locks = context.lockman.getLocks(transaction);
            for (Lock l : locks) {
                if (LockType.canBeParentLock(lockToRelease.lockType, (l.lockType))) {
//                if (lockToRelease.lockType.equals(LockType.parentLock(l.lockType))) {
                    throw new InvalidLockException("violate multigranularity locking constraints");
                }
            }
        }

//        if (!numChildLocks.getOrDefault(transaction.getTransNum(), 0).equals(0)) {
//            throw new InvalidLockException("The lock cannot be released.");
//        }


        lockman.release(transaction, name);
        // minus map
        LockContext pointer = this.parent;
        long transNum = transaction.getTransNum();
        while (pointer != null) {
            Integer current = pointer.numChildLocks.get(transNum);
            if (current == null)
                throw new InvalidLockException("不知道是个啥占个位置吧，反正也到不了");
            else
                pointer.numChildLocks.put(transNum, current - 1);
            pointer = pointer.parent;
        }

        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A //and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("readonly");
        List<Lock> locksFromTrans = lockman.getLocks(transaction);
        if (locksFromTrans.isEmpty()) throw new NoLockHeldException("NoLockHeldException");
        for (Lock l : locksFromTrans) {
            if (l.lockType.equals(newLockType) && l.name.equals(name))
                throw new DuplicateLockRequestException("transaction` already has a `newLockType` lock");
        }
        // get the lock to be replaced
        Lock lockToReplace = null;
        for (Lock l : locksFromTrans) {
            if (l.name.equals(getResourceName())) {
                lockToReplace = l;
            }
        }
        if (lockToReplace == null) throw new NoLockHeldException("no lock here !!");

        if ((LockType.substitutable(newLockType, lockToReplace.lockType) && !newLockType.equals(lockToReplace.lockType)) ||
                // condition 2
                (hasSIXAncestor(transaction) && newLockType.equals(LockType.SIX)) &&
                        (lockToReplace.lockType.equals(LockType.IX) || lockToReplace.lockType.equals(LockType.IS) || lockToReplace.lockType.equals(LockType.S))) {
            // For promotion to SIX from
            //     * IS/IX, all S and IS locks on descendants must be simultaneously
            //     * released
            if (newLockType.equals(LockType.SIX) &&
                    (lockToReplace.lockType.equals(LockType.IX) || lockToReplace.lockType.equals(LockType.IS))) {
                List<ResourceName> resourceNames = sisDescendants(transaction);

                List<ResourceName> toBeDeleted = new ArrayList<>();
                for (ResourceName resourceName : resourceNames) {
                    toBeDeleted.add(resourceName);
                }
                toBeDeleted.add(name);
                this.lockman.acquireAndRelease(transaction, name, newLockType, toBeDeleted);

            } else {
                this.lockman.promote(transaction, name, newLockType);
            }

        } else {
            throw new InvalidLockException("invalid operation");
        }
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("readOnly");
        // get locks from this transaction
        List<Lock> locksFromTrans = this.lockman.getLocks(transaction);
        if (locksFromTrans.isEmpty()) {
            throw new NoLockHeldException("no locks in transaction");
        }
        boolean noLock = true;
        Lock curretnLock = null;
        for (Lock l : locksFromTrans) {
            if (l.name.equals(getResourceName())) {
                noLock = false;
                curretnLock = l;
                break;
            }
        }
        if (noLock) throw new NoLockHeldException("no lock at this level");
        List<ResourceName> resourceNames = sisDescendants(transaction);
        List<Lock> decedentLocks = locksFromTrans.stream().filter(lock -> resourceNames.contains(lock.name))
                .collect(Collectors.toList());

        if (decedentLocks.isEmpty()) {
            if (curretnLock.lockType.equals(LockType.IX)) {
//                release(transaction);
                lockman.replaceLock(transaction, name, LockType.X);
            } else if (curretnLock.lockType.equals(LockType.IS)) {
//                release(transaction);
                lockman.replaceLock(transaction, name, LockType.S);
            }
        } else {
            // get the lock with the smallest scope
            Lock updatingLock = decedentLocks.get(0);
            int except = 0;
            for (int i = 1; i < decedentLocks.size(); i++) {
                if (LockType.substitutable(decedentLocks.get(i).lockType, updatingLock.lockType)) {
                    updatingLock = decedentLocks.get(i);
                    except = i;
                }
            }
            // release all locks
            for (int i = 0; i < decedentLocks.size(); i++) {
//                if (i == except) continue;
                List<String> names = decedentLocks.get(i).name.getNames();
                String s = names.get(names.size() - 1);
                childContext(s).release(transaction);
            }
            // update the lockType
            lockman.replaceLock(transaction, name, updatingLock.lockType);

            numChildLocks.put(transaction.getTransNum(), 0);
        }

        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        List<Lock> locks = this.lockman.getLocks(transaction);
        for (Lock l : locks) {
            if (l.name.equals(getResourceName())) {
                return l.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        if (this.parent == null || !getExplicitLockType(transaction).equals(LockType.NL)) {
            if (getExplicitLockType(transaction).equals(LockType.X) || getExplicitLockType(transaction).equals(LockType.SIX) || getExplicitLockType(transaction).equals(LockType.S)) {
                return getExplicitLockType(transaction);
            } else {
                return LockType.NL;
            }

        }

        return this.parent.getEffectiveLockType(transaction);
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (this.parent == null) return false;
        List<Lock> locks = this.parent.lockman.getLocks(transaction);
        boolean b = false;
        for (Lock l : locks) {
            if (l.lockType.equals(LockType.SIX)) {
                b = true;
                break;
            }
        }
        return this.parent.hasSIXAncestor(transaction) || b;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // get all locks of parents
        Set<ResourceName> toBeDel = new HashSet<>();
        LockContext pointer = this;
        while (pointer != null) {
             toBeDel.add(pointer.name);
            pointer = pointer.parent;
        }
        List<ResourceName> collected = this.lockman.getLocks(transaction).stream()
                .filter(lock -> ((lock.lockType == LockType.S || lock.lockType == LockType.IS) && !toBeDel.contains(lock.name)))
                .map(lock -> lock.name)
                .collect(Collectors.toList());
        return collected;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

