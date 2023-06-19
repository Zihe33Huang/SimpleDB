package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * @author huangzihe
 * @date 2023/6/13 11:34 PM
 */
public class LockManager {

    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> pageLocks;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, Permissions p) throws InterruptedException {
        ConcurrentHashMap<TransactionId, PageLock> locks = pageLocks.get(pageId);
        // 1. no any locks on this page
        if (locks == null || locks.size() == 0) {
            PageLock pageLock = new PageLock(tid, p);
            if (locks == null) {
                locks = new ConcurrentHashMap<>();
            }
            locks.put(tid, pageLock);
            pageLocks.put(pageId, locks);
            return true;
        }

        // 2. locks already exist, check number of locks
        if (locks.size() == 1) {
            //  check if this lock belongs to current transaction
            PageLock existLock = locks.get(tid);
            // 2.1 this single lock belongs to current transaction
            if (existLock != null) {
                if (p == Permissions.READ_ONLY) {
                    System.out.println("");
                    return true;
                } else if (p == Permissions.READ_WRITE) {
                    existLock.setType(p);
                    if (existLock.getType() == Permissions.READ_WRITE) {
                        System.out.println("Already got exclusive lock");
                    } else {
                        System.out.println("Upgrade share to exclusive");
                    }
                    return true;
                }
            } else {
                Iterator<PageLock> iterator = locks.values().iterator();
                existLock  = iterator.next();
                // 2.2 this single lock does not belong to current transaction
                if (existLock.getType() == Permissions.READ_ONLY) {
                    if (p == Permissions.READ_ONLY) {
                        PageLock addLock = new PageLock(tid, p);
                        locks.put(tid, addLock);
                        pageLocks.put(pageId, locks);
                        System.out.println("add share lock");
                        return true;
                    } else if (p == Permissions.READ_WRITE) {
                        wait(50);
                        System.out.println("There is already a share lock, acquire lock fails");
                        return false;
                    }
                } else if (existLock.getType() == Permissions.READ_WRITE) {
                    wait(50);
                    System.out.println("Already exists READ_WRITE lcok, write");
                    return false;
                }
            }
        }

        // 3. more than one locks on this page, must be multiple share lock
        PageLock one = locks.values().iterator().next();
        if (p == Permissions.READ_ONLY) {
            PageLock pageLock = new PageLock(tid, p);
            locks.put(tid, pageLock);
            pageLocks.put(pageId, locks);
//            System.out.println("multiple share locks, add one more");
            return true;
        } else if (p == Permissions.READ_WRITE){
            this.wait(50);
//            System.out.println("multiple share locks, wait");
            return false;
        }
        System.out.println("不可能到这一步");
        return false;
    }

    public synchronized boolean isHoldLock(PageId pageId, TransactionId transactionId) {
        ConcurrentHashMap<TransactionId, PageLock> locks = pageLocks.get(pageId);
        return locks == null ? false : locks.get(transactionId) == null;
    }


    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        String thread = Thread.currentThread().getName();
        ConcurrentHashMap<TransactionId, PageLock> locks = pageLocks.get(pid);
        if (locks == null || locks.size() == 0 || locks.get(tid) == null) {
//            System.out.println("Current transaction does not have lock on this page");
        }
        locks.remove(tid);
        System.out.println(thread + " release lock");
        this.notifyAll();
    }

    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> ids = pageLocks.keySet();
        for (PageId pageId : ids) {
            releaseLock(pageId, tid);
        }
    }



    class PageLock {
        private TransactionId tid;

        private Permissions p;
        public PageLock(TransactionId tid, Permissions p) {
            this.tid = tid;
            this.p = p;
        }

        public Permissions getType() {
            return this.p;
        }
        public void setType(Permissions p) {
            this.p = p;
        }
    }
}
