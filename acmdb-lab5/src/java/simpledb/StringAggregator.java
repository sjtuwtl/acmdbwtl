package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private HashMap<Field, Integer> sum, max, min, cnt, avg;
    private HashMap<Field, Tuple> ans;

    TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (this.gbfield == NO_GROUPING)
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        sum = new HashMap<Field, Integer>();
        max = new HashMap<Field, Integer>();
        min = new HashMap<Field, Integer>();
        cnt = new HashMap<Field, Integer>();
        avg = new HashMap<Field, Integer>();

        ans = new HashMap<Field, Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = null;
        if (gbfield != NO_GROUPING)
            field = tup.getField(gbfield);
        if (!cnt.containsKey(field))
            cnt.put(field, 0);
        cnt.put(field, cnt.get(field) + 1);
        if (gbfield == NO_GROUPING) {
            Tuple tuple = new Tuple(tupleDesc);
            if (what == Op.COUNT)
                tuple.setField(0, new IntField(cnt.get((field))));
            ans.put(field, tuple);
        } else {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, field);
            if (what == Op.COUNT)
                tuple.setField(1, new IntField(cnt.get((field))));
            ans.put(field, tuple);
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(tupleDesc, ans.values());
    }

}
