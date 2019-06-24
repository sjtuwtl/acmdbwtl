package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    private HashMap<Integer, String> tables = new HashMap<>();
    private HashMap<Integer, DbFile> files = new HashMap<>();
    private HashMap<Integer, String> keys = new HashMap<>();
    private HashMap<Integer, Integer> ids = new HashMap<>();

/*    private Vector<String> tables = new Vector<>();
    private Vector<DbFile> files = new Vector<>();
    private Vector<String> keys = new Vector<>();
    private Vector<Integer> ids = new Vector<>();*/
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        /*
        if (tables.contains(name)) {
            tables.remove(name);
            files.remove(name);
            keys.remove(name);
            ids.remove(name);
        }
        tables.setElementAt(name, file.getId());
        files.setElementAt(file, file.getId());
        keys.setElementAt(pkeyField, file.getId());
        ids.setElementAt(file.getId(), file.getId()); */
        if (tables.containsValue(name))
        {
            Integer conflict = null;
            for (Map.Entry<Integer, String> entry : tables.entrySet())
                if (entry.getValue().equals(name))
                {
                    conflict = entry.getKey();
                    break;
                }
            tables.remove(conflict);
            files.remove(conflict);
            keys.remove(conflict);
            ids.remove(conflict);
        }
        tables.put(file.getId(), name);
        files.put(file.getId(), file);
        keys.put(file.getId(), pkeyField);
        ids.put(file.getId(), file.getId());
        // some code goes here
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        for (HashMap.Entry<Integer, String> entry : tables.entrySet())  {
            if (entry.getValue().equals(name))
                return entry.getKey();
        }
        throw new NoSuchElementException("Table does not exist");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile dbFile = null;
        if (tables.get(tableid) != null)
            dbFile = files.get(tableid);
        if (dbFile == null)
            throw new NoSuchElementException("Table does not exist");
        return dbFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile dbFile = null;
        if (tables.get(tableid) != null)
            dbFile = files.get(tableid);
        if (dbFile == null)
            throw new NoSuchElementException("Table does not exist");
        return dbFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        int num = 0;
        if (tables.get(tableid) != null)
            num = tableid;
        if (num == 0)
            throw new NoSuchElementException("Table does not exist");
        return keys.get(num);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return ids.values().iterator();
    }

    public String getTableName(int tableid) {
        // some code goes here
        String s = "";
        if (tables.get(tableid) != null)
            s = tables.get(tableid);
        if (s.equals(""))
            return null;
        return s;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        tables.clear();
        keys.clear();
        ids.clear();
        // some code goes here
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

