package simpledb.storage;

import oracle.jrockit.jfr.Recording;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
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
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPage heapPage = null;
        int page_size = BufferPool.getPageSize();
        int pgno = pid.getPageNumber();
        byte[] page = new byte[page_size];
        try {
            RandomAccessFile r = new RandomAccessFile(file, "r");
            r.seek(page_size * pgno);
            if ( r.read(page) == -1) {
                return heapPage;
            }
            heapPage = new HeapPage((HeapPageId) pid, page);
            r.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return heapPage;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        byte[] pageData = page.getPageData();
        RandomAccessFile r = new RandomAccessFile(this.file, "rw");
        r.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
//        r.skipBytes(page.getId().getPageNumber() * BufferPool.getPageSize());
        r.write(pageData);
        r.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        int num = this.numPages();
        for (int i = 0; i < num; i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);

            int numEmptySlots = page.getNumEmptySlots();
            if (numEmptySlots == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
                continue;
            }
                page.insertTuple(t);
                pages.add(page);
                return pages;
        }

        // Append
        BufferedOutputStream bf = new BufferedOutputStream(new FileOutputStream(this.file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bf.write(emptyPageData);
        bf.close();

//        HeapPageId pageId = new HeapPageId(getId(), numPages() - 1);
        HeapPageId pageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
//        HeapPage newPage = new HeapPage(pageId, emptyPageData);

        newPage.insertTuple(t);
        pages.add(newPage);
//        writePage(newPage);
//        pages.add(newPage);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        int pageNumber = t.getRecordId().getPageId().getPageNumber();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;

        /**
         * 存储了堆文件迭代器
         */
        private Iterator<Tuple> tupleIterator;
        private int index;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            tupleIterator = getTupleIterator(index);
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws TransactionAbortedException, DbException{
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapFile %d  does not exist in page[%d]!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub

            if(tupleIterator == null){
                return false;
            }

            while(!tupleIterator.hasNext()){
                index++;
                if(index < heapFile.numPages()){
                    tupleIterator = getTupleIterator(index);
                }else{
                    return false;
                }
            }
            return true;

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null || !tupleIterator.hasNext()){
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }

    }

}

