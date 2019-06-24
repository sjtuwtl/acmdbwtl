package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private DbIterator child;
    private int number;
    private TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.transactionId = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (number == -1)
            return null;
        while (child.hasNext()) {
            Tuple tmp = child.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, tmp);
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
