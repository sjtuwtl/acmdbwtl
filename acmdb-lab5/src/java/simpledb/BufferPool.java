package simpledb;

import javax.xml.crypto.Data;
import java.awt.*;
import java.io.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int Number = 0;
    private ConcurrentHashMap<PageId, Page> pages;
    public class FIFOList {

        public class Node {
            public PageId pageId;
            public Node next;

            public Node(PageId pageId) {
                this.pageId = pageId;
            }
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        Number = numPages;
        pages = new ConcurrentHashMap<>(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    public class Locker {
        private ConcurrentHashMap<PageId, CopyOnWriteArrayList<TransactionId>> SharedLock;
        private ConcurrentHashMap<PageId, CopyOnWriteArrayList<TransactionId> > ExclusiveLock;
        private int num = 0;
        public Locker() {
            SharedLock = new ConcurrentHashMap<>();
            ExclusiveLock = new ConcurrentHashMap<>();
        }

        private synchronized void addLock(PageId pageId, TransactionId transactionId, Permissions permissions) {
            num++;
            if (permissions == Permissions.READ_ONLY)
                if (SharedLock.containsKey(pageId))
                    SharedLock.get(pageId).add(transactionId);
                else {
                    CopyOnWriteArrayList<TransactionId> tmp = new CopyOnWriteArrayList<>();
                    tmp.add(transactionId);
                    SharedLock.put(pageId, tmp);
                }
            else if (ExclusiveLock.containsKey(pageId))
                ExclusiveLock.get(pageId).add(transactionId);
            else {
                CopyOnWriteArrayList<TransactionId> tmp = new CopyOnWriteArrayList<>();
                tmp.add(transactionId);
                ExclusiveLock.put(pageId, tmp);
            }
        }

        public synchronized boolean Conflict(PageId pageId, TransactionId transactionId, Permissions permissions) {
            if (num < 100)
                return false;
            if (permissions == Permissions.READ_ONLY) {
                if (ExclusiveLock.containsKey(pageId))
                    for (TransactionId tid : ExclusiveLock.get(pageId))
                        if (!tid.equals(transactionId))
                            if (tid.hashCode() < transactionId.hashCode())
                                return true;
            }
            else {
                if (ExclusiveLock.containsKey(pageId)) {
                    for (TransactionId tid : ExclusiveLock.get(pageId))
                        if (!tid.equals(transactionId))
                            if (tid.hashCode() < transactionId.hashCode())
                                return true;
                }
                if (SharedLock.containsKey(pageId)) {
                    for (TransactionId tid : SharedLock.get(pageId))
                        if (!tid.equals(transactionId))
                            if (tid.hashCode() < transactionId.hashCode())
                                return true;
                }

            }
            return false;
        }

        public synchronized boolean CheckLock(PageId pageId, TransactionId transactionId, Permissions permissions) {
            int privateShare = 0, privateExclusive = 0, publicShare = 0, publicExclusive = 0;
            if (SharedLock.containsKey(pageId))
                for (TransactionId tid : SharedLock.get(pageId))
                    if (tid.equals(transactionId))
                        privateShare++;
                    else
                        publicShare++;
            if (ExclusiveLock.containsKey(pageId))
                for (TransactionId tid : ExclusiveLock.get(pageId))
                    if (tid.equals(transactionId))
                        privateExclusive++;
                    else
                        publicExclusive++;
            if (permissions == Permissions.READ_ONLY){
                if (privateShare == 1 && publicExclusive == 0)
                    return true;
                else if (privateShare == 0 && publicExclusive == 0) {
                    addLock(pageId, transactionId, permissions);
                    return true;
                }
                else if (publicExclusive == 1)
                    return false;
            }
            else {
                if (privateExclusive == 1)
                    return true;
                else if (privateExclusive == 0 && publicExclusive == 0 && publicShare == 0) {
                    addLock(pageId, transactionId, permissions);
                    return true;
                }
                else if (publicExclusive == 1 || publicShare >= 1)
                    return false;
            }
            return false;
        }

        public synchronized void releaseLock(PageId pageId, TransactionId transactionId) {
            if (SharedLock.containsKey(pageId)) {
                SharedLock.get(pageId).remove(transactionId);
                if (SharedLock.get(pageId).isEmpty())
                    SharedLock.remove(pageId);
            }
            if (ExclusiveLock.containsKey(pageId)) {
                ExclusiveLock.get(pageId).remove(transactionId);
                if (ExclusiveLock.get(pageId).isEmpty())
                    ExclusiveLock.remove(pageId);
            }
        }

        public synchronized void releaseTransaction(TransactionId transactionId) {
            for (Map.Entry<PageId, CopyOnWriteArrayList<TransactionId> > entry : SharedLock.entrySet())
                if (entry.getValue().contains(transactionId))
                    releaseLock(entry.getKey(), transactionId);
            for (Map.Entry<PageId, CopyOnWriteArrayList<TransactionId> > entry : ExclusiveLock.entrySet())
                if (entry.getValue().contains(transactionId))
                    releaseLock(entry.getKey(), transactionId);
        }
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    private Locker locker = new Locker();

    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        boolean lock = locker.CheckLock(pid, tid, perm);
        int num = 0;
        while (!lock) {
            num++;
            if (num == 20 || locker.Conflict(pid, tid, perm))
                throw new TransactionAbortedException();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
            lock = locker.CheckLock(pid, tid, perm);
        }
        try {
            /*Page page;
            if (pages.containsKey(pid)) {
                return pages.get(pid);
            }
            else {
                while (pages.size() >= Number)
                    evictPage();

                page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pages.put(pid, page);
                return page;

            }*/
            if (pages.containsKey(pid)) {
                return pages.get(pid);
            }
            if (pages.size() >= Number) {
                Iterator<PageId> iter = pages.keySet().iterator();
                PageId curPage = iter.next();
                while (pages.get(curPage).isDirty() != null) {
                    if (!iter.hasNext())
                        throw new DbException("no enough clean page");
                    curPage = iter.next();
                }
                flushPage(curPage);
                pages.remove(curPage);
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pages.put(pid, page);
                return page;
            }
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pages.put(pid, page);
            return page;
        } catch (Exception e) {
            throw new DbException("Page Conflict");
        }

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        locker.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        locker.releaseTransaction(tid);
        if (commit)
            flushPages(tid);
        else {
            ArrayList<Page> olds = new ArrayList<>();
            for (Page page: pages.values())
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    Page old = Database.getCatalog().getDatabaseFile(page.getId().getTableId()).readPage(page.getId());
                    old.markDirty(false, null);
                    olds.add(old);
                }
            for (Page page: olds)
                pages.put(page.getId(), page);
        }
    }

    private void Tupletmp(TransactionId tid, ArrayList<Page> pageArrayList)
            throws DbException, IOException, TransactionAbortedException{
        Page curPage;
        Iterator<Page> iter = pageArrayList.iterator();
        while (iter.hasNext()) {
            curPage = iter.next();
            curPage.markDirty(true, tid);
            if (pages.containsKey(curPage.getId())) {
                pages.put(curPage.getId(), curPage);
                continue;
            }
            if (pages.size() >= Number) {
                Iterator<PageId> it = this.pages.keySet().iterator();
                PageId pageId = it.next();
                boolean mark = false;
                while (this.pages.get(pageId).isDirty() != null) {
                    if (!it.hasNext()) {
                        mark = true;
                        break;
                    }
                    pageId = it.next();
                }
                if (mark) {
                    flushPage(curPage.getId());
                    continue;
                }
                flushPage(pageId);
                this.pages.remove(pageId);
            }
            this.pages.put(curPage.getId(), curPage);
        }
        /*for (Page p : pageArrayList) {
            PageId pageId = p.getId();
            p.markDirty(true, tid);
            if (pages.containsKey(pageId)) {
                pages.remove(pageId);
                Number--;
            }
            while (pages.size() >= Number)
                evictPage();
            pages.put(p.getId(), p);
        }*/
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */

    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageArrayList = f.insertTuple(tid, t);
        Tupletmp(tid, pageArrayList);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pageArrayList = f.deleteTuple(tid, t);
        Tupletmp(tid, pageArrayList);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId: pages.keySet()) {
            Page page = pages.get(pageId);
            if (page.isDirty() != null) {
                page.markDirty(false, null);
                Database.getCatalog().getDatabaseFile(pageId.getTableId()).writePage(page);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pages.get(pid);
        if (page == null || page.isDirty() == null)
            return;
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        f.writePage(page);
        page.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (pages.isEmpty())
            return;
        for (Page page: pages.values())
            if (page.isDirty() != null && page.isDirty().equals(tid))
                flushPage(page.getId());
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId  pageid = pages.keySet().iterator().next();
        try {
            flushPage(pageid);
            pages.remove(pageid);
        }
        catch (IOException e) {
            throw new DbException("Can not write file on disk" );
        }
        pages.remove(pageid);
    }

}
