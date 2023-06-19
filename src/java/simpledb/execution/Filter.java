package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    OpIterator it;

    Predicate p;

    public Filter(Predicate p, OpIterator child) {
        this.it = child;
        this.p = p;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return it.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.open();
        super.open();
    }

    public void close() {
        it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
//        if (!it.hasNext()) {
//            return null;
//        }
//        Tuple next = it.next();
//        while (!this.p.filter(next)) {
//            if (!it.hasNext()) {
//                return null;
//            }
//            next = this.it.next();
//        }
//        return next;
        while (it.hasNext()) {
            Tuple next = it.next();
            if (p.filter(next)) {
                return next;
            }
        }
        return null;
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
