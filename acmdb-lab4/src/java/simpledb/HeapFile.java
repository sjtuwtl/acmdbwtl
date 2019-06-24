package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file,"r");
            randomAccessFile.seek(BufferPool.getPageSize() * pid.pageNumber());
            byte[] content = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(content);
            randomAccessFile.close();
            return new HeapPage((HeapPageId)pid, content);
        } catch (Exception e) {
            throw new IllegalArgumentException("No such a Page!");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file));
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(page.getId().pageNumber() * BufferPool.getPageSize());
        randomAccessFile.write(page.getPageData(), 0, BufferPool.getPageSize());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public synchronized ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                dirtyPages.add(heapPage);
                return dirtyPages;
            }
        }
        HeapPage heapPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        dirtyPages.add(heapPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public synchronized  ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        heapPage.markDirty(true, tid);
        dirtyPages.add(heapPage);
        return dirtyPages;
    }

    public class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int pageCnt;
        private Iterator<Tuple> iter;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (iter == null)
                return false;
            while (!iter.hasNext())
            {
                ++pageCnt;
                if (pageCnt >= numPages())
                {
                    return false;
                }
                iter = ((HeapPage) Database.getBufferPool().getPage(
                        tid, new HeapPageId(getId(), pageCnt), Permissions.READ_WRITE)).iterator();
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            rewind();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageCnt = 0;
            iter = ((HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), 0), Permissions.READ_WRITE)).iterator();
        }
        @Override
        public void close() {
            iter = null;
            pageCnt = 0;
        }

    }

    // see DbFile.java for javadocs


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

