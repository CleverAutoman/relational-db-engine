package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     *
     * - The current lock type can effectively substitute the requested type ---> currentType
     * - The current lock type is IX and the requested lock is S --> SIX
     * - The current lock type is an intent lock ---> IX/ IS
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that //ensures you have
     * the appropriate locks on all ancestors.//
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (requestType.equals(LockType.NL)) return;
        // ensure ancestors Lock
        ensureAncestorsLock(lockContext, requestType);

        // the current lock type can effectively substitute the requested type ---> currentType
        if (LockType.substitutable(effectiveLockType, requestType)) {
            // do nothing
            return;
        // The current lock type is IX and the requested lock is S --> SIX
        } else if ((explicitLockType.equals(LockType.IX) || effectiveLockType.equals(LockType.IX)) && requestType.equals(LockType.S)) {

            lockContext.promote(transaction, LockType.SIX);
        // The current lock type is an intent lock ---> IX/ IS
        } else if (effectiveLockType.isIntent()) {
            // change to requestType
            List<ResourceName> lst = new ArrayList<>();
            lst.add(lockContext.name);
            lockContext.lockman.acquireAndRelease(transaction, lockContext.name, requestType, lst);

        // other logics more than the requirements
        } else if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            // promote situation
            if (LockType.substitutable(effectiveLockType, explicitLockType)) {
                lockContext.promote(transaction, requestType);
            // acquire and release situation
            } else {
//                lockContext.escalate(transaction);
                List<ResourceName> names = lockContext.lockman.getLocks(transaction).stream().map(context -> context.name).collect(Collectors.toList());

                Collection<LockContext> childContexts = lockContext.children.values();

                List<ResourceName> lst = new ArrayList<>();
                lst.add(lockContext.name);
                lst.addAll(childContexts.stream().map(context -> context.name).filter(context -> names.contains(context)).collect(Collectors.toList()));
                lockContext.lockman.acquireAndRelease(transaction, lockContext.name, requestType, lst);
            }
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want

    private static void ensureAncestorsLock(LockContext lockContext, LockType lockType) {
        // make all parent's lock to intentent lockType
        // get the intention type of lockType
        LockType intentType = intentType(lockType);

        // get current transaction
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext pointer = lockContext.parent;
        List<LockContext> precedentContexts = new ArrayList<>();

        while (pointer != null) {
            // current effective lock type
            precedentContexts.add(pointer);
            pointer = pointer.parent;
        }
        Collections.reverse(precedentContexts);

        for (LockContext context : precedentContexts) {
            LockType effectiveLockType = context.getEffectiveLockType(TransactionContext.getTransaction());
            LockType explicitLockType = context.getExplicitLockType(TransactionContext.getTransaction());

            if (!LockType.substitutable(intentType, effectiveLockType)
                    || !LockType.substitutable(intentType, explicitLockType)
                    || intentType.equals(explicitLockType)) {
                // do nothing
                return;
            } else {
                // change point's lock to intentionType
                // ix is the parent of is ？
                // all the lock types are nl
                if (context.getEffectiveLockType(transaction).equals(LockType.NL)
                        && context.getExplicitLockType(transaction).equals(LockType.NL)) {
                    context.acquire(transaction, intentType);

                } else if (LockType.substitutable(effectiveLockType, explicitLockType)
                        || context.getEffectiveLockType(transaction).equals(LockType.NL)) {
                    context.promote(transaction, intentType);

                } else {
                    List<ResourceName> lst = new ArrayList<>();
                    lst.add(context.name);
                    context.lockman.acquireAndRelease(transaction, context.name, intentType, lst);

                }
            }
        }
    }

    private static LockType intentType(LockType lockType) {
        if (lockType.equals(LockType.S)) {
            return LockType.IS;
        } else if (lockType.equals(LockType.X)) {
            return LockType.IX;
        } else {
            return LockType.NL;
        }
    }
}
