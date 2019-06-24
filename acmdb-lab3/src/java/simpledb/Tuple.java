package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private RecordId r = null;
    private ArrayList<Field> allfield = new ArrayList<>();

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        for (int i = 0; i < td.numFields(); i++)
            allfield.add(new IntField(0));
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return r;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        r = rid;
        // some code goes here
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        allfield.set(i, f);
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return allfield.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < tupleDesc.numFields() - 1; i++)
            s += allfield.get(i).toString() + "\t";
        s += allfield.get(tupleDesc.numFields() - 1);
        return s;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        // some code goes here
        return allfield.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        tupleDesc = td;
        for (int i = 0; i < td.numFields(); i++)
            allfield.add(new IntField(0));
        // some code goes here
    }

    public boolean equals(Object o) {
        // some code goes here
        Tuple td;
        try {
            td = (Tuple) o;
        }
        catch (Exception e) {
            return  false;
        }
        if (td == null)
            return false;
        if (!td.tupleDesc.equals(this.tupleDesc))
            return false;
        for (int i = 0; i < this.allfield.size(); ++i)
            if (!this.getField(i).equals(td.getField(i)))
                return false;
        return true;
    }
}
