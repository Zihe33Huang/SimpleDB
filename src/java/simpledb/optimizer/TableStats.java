package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private Map<Integer, StringHistogram> stringHistogramMap;

    private Map<Integer, IntHistogram> integerIntHistogramMap;

    HeapFile dbFile;

    private int ioCostPerPage;

    private int tableId;

    private int totalTuple;

    private int totalPages;

    private TupleDesc tupleDesc;

    private List<Tuple> tupleList;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        this.totalPages = this.dbFile.numPages();
        this.tableId = tableid;
        this.tupleDesc = this.dbFile.getTupleDesc();
        this.tupleList = new ArrayList<>();
        this.integerIntHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        // key for filed index, value for all values in this field
        HashMap<Integer, Integer> minimum = new HashMap<>();
        HashMap<Integer, Integer> maximum = new HashMap<>();
        DbFileIterator tupleIt = dbFile.iterator(new TransactionId());
        int numFields = tupleDesc.numFields();
        for (int i = 0; i < numFields; i++) {
            minimum.put(i, Integer.MAX_VALUE);
            maximum.put(i, Integer.MIN_VALUE);
        }
        try {
            tupleIt.open();
        while (tupleIt.hasNext()) {
            this.totalTuple++;
            Tuple tuple = tupleIt.next();
            tupleList.add(tuple);
            for (int i = 0; i < numFields; i++) {
                    Field field = tuple.getField(i);
                    if (!(field instanceof IntField)) {
                        continue;
                    }
                    IntField intField = (IntField) field;
                    int value = intField.getValue();
                    Integer curMin = minimum.get(i);
                    Integer curMax = maximum.get(i);
                    if (value < curMin) {
                        minimum.put(i, value);
                    }
                    if (value > curMax) {
                        maximum.put(i, value);
                    }
            }
        }} catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < numFields; i++) {
            Type fieldType = this.tupleDesc.getFieldType(i);
            Iterator<Tuple> iterator = tupleList.iterator();
            if (fieldType.equals(Type.INT_TYPE)) {
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minimum.get(i), maximum.get(i));
                integerIntHistogramMap.put(i, intHistogram);
                while (iterator.hasNext()) {
                    Tuple tuple = iterator.next();
                    IntField field = (IntField) tuple.getField(i);
                    intHistogram.addValue(field.getValue());
                }
            } else if (fieldType.equals(Type.STRING_TYPE)) {
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringHistogramMap.put(i, stringHistogram);
                while (iterator.hasNext()) {
                    Tuple tuple = iterator.next();
                    StringField field = (StringField) tuple.getField(i);
                    stringHistogram.addValue(field.getValue());
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.ioCostPerPage * this.totalPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        int i = (int) (selectivityFactor * totalTuple);
        return i;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant instanceof IntField) {
            IntField intField = (IntField) constant;
            IntHistogram intHistogram = this.integerIntHistogramMap.get(field);
            return intHistogram.estimateSelectivity(op, intField.getValue());
        } else if (constant instanceof StringField) {
            StringField stringField = (StringField) constant;
            StringHistogram stringHistogram = this.stringHistogramMap.get(stringField);
            return stringHistogram.estimateSelectivity(op, stringField.getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
