package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    OpIterator it;

    int tableId;

    TransactionId t;

    TupleDesc tupleDesc;

    boolean hasInserted;

    /**
     * @param child Tuples that will be inserted into table
     * @param tableId   Table to insert
     * @throws DbException
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.it = child;
        this.t = t;
        this.tableId = tableId;
        this.hasInserted = false;

        Type[] types = {Type.INT_TYPE};
        this.tupleDesc = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.it.open();
        super.open();
    }

    public void close() {
       this.it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
       this.it.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasInserted) {
            return null;
        }
        hasInserted = true;
        int cnt = 0;
        while (this.it.hasNext()) {
            Tuple next = it.next();
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, next);
                cnt++;
            } catch (IOException ioException) {
                throw new DbException("");
            }
        }
        // Zihe: set how many tuples been inserted
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0, new IntField(cnt));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
