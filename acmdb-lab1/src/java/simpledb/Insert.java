package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private DbIterator child;
    private int tableId;
    private int number;
    private TupleDesc tupleDesc;

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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        number = 0;
        Type[] tmptype = new Type[1];
        tmptype[0] = Type.INT_TYPE;
        tupleDesc = new TupleDesc(tmptype);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        // some code goes here
        if (number == -1)
            return null;
        while (child.hasNext()) {
            Tuple tmp = child.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, tmp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            number++;
        }
        Tuple ans = new Tuple(tupleDesc);
        ans.setField(0, new IntField(number));
        number = -1;
        return ans;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] tmp = new DbIterator[]{child};
        return tmp;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
