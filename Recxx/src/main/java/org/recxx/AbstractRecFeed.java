package org.recxx;

import com.sun.istack.internal.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Boolean.valueOf;

/**
 * Abstract class created to represent common methods for handling a
 * java.sql.ResultSet, such as processing it, creating keys and handling the
 * ResultSetMetaData All FacadeWorker class's should extend from this and make
 * a concrete implementation.
 */
public abstract class AbstractRecFeed {

    Logger LOGGER = Logger.getLogger(AbstractRecFeed.class.getName());

    protected String propertiesFile = "";
    protected String prefix = "";
    protected Properties props = null;
    protected Properties superProps = null;

    protected boolean keyColumnPositionsSet = false;
    protected List<Integer> keyColumnPositions;
    protected String[] columns;
    protected HashMap data;

    public DecimalFormat decimalFormatter = new DecimalFormat("##,##0");

    /**
     * Constructor for AbstractRecFeed.
     */
    public AbstractRecFeed() {
        super();
    }

    /**
     * A general initalisation which sets the properties and the outputloggers
     *
     * @param prefix         prefix to be used when querying the properties file
     * @param propertiesFile location of properties file to be used
     */
    public void init(String prefix, String propertiesFile) {
        File file = new File(propertiesFile);
        if (!file.exists() || !file.canRead()) {
            throw new RuntimeException("cannot find or read properties file please check " + propertiesFile);
        }
        this.propertiesFile = propertiesFile;
        this.prefix = prefix;
        props = new Properties();
        props.getProperty(propertiesFile);
        superProps = new Properties();
        try {
            superProps.load(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("cannot find or read properties file please check " + propertiesFile);
        } catch (IOException ioe) {
            throw new RuntimeException("problem loading properties file " + propertiesFile);
        }
    }

    /**
     * Checks to see if the columns specified in the key[], are in the columns[]
     *
     * @param keys    key to look for in the data set
     * @param columns columns to use in the reconciliation
     * @return true if the key is in columns, false otherwise.
     */
    public static boolean keysPresentInColumns(String[] keys, String[] columns) {
        boolean allKeysPresent = true;
        for (String key : keys) {
            if (!containsKey(columns, key)) {
                allKeysPresent = false;
            }
        }
        return allKeysPresent;
    }

    private static boolean containsKey(String[] columns, String key) {
        boolean columnExists = false;
        for (String column : columns) {
            if (key.equalsIgnoreCase(column)) {
                columnExists = true;
            }
        }
        return columnExists;
    }

    /**
     * Converts the string key back into an array of key columns
     *
     * @param keys      key to convert
     * @param delimiter used for the split
     * @return String[]
     */
    public static String[] convertStringKeyToArray(String keys, @Nullable String delimiter) {
        if (delimiter == null) {
            delimiter = CONSTANTS.DELIMITER;
        }
        return keys.split(delimiter);
    }

    public static String[] convertStringKeyToArray(String keys) {
        return convertStringKeyToArray(keys, null);
    }

    /**
     * returns an array of column names, given the ResultSetMetaData.
     *
     * @param meta the meta data to use for the column extraction
     * @return columns names of the columns
     * @throws java.sql.SQLException SQL Error
     */
    public static String[] getColumnsData
    (ResultSetMetaData
             meta) throws SQLException {
        // pull out the data columns first
        String[] columns = new String[meta.getColumnCount()];
        for (int j = 1; j <= meta.getColumnCount(); j++) {
            columns[j - 1] = meta.getColumnName(j);
        }

        return columns;
    }

    /**
     * returns an array of column class names (such as java.lang.Double,
     * java.lang.String), given the ResultSetMetaData.
     *
     * @param meta The metadata to use for the column extraction.
     * @return columns return the column types and the names in an 2 column array.
     * @throws java.sql.SQLException SQL Error
     */
    public static String[] getColumnsClassNameData
    (ResultSetMetaData
             meta)
            throws SQLException {
        // pull out the data columns first
        String[] columns = new String[meta.getColumnCount()];

        for (int j = 1; j <= meta.getColumnCount(); j++) {
            columns[j - 1] = meta.getColumnClassName(j);
        }

        return columns;
    }

    /**
     * Given an array of column names (columns), an array of keys (keyColumns)
     * and the data (row), return an unique key
     *
     * @param columns    column names
     * @param keyColumns array of keys/
     * @param row        data row
     * @return String    a unique key
     */

    public String generateKey(String[] columns, String[] keyColumns,
                              ArrayList row) {
        String key = "";

        if (!keyColumnPositionsSet) {
            // first time in, set the positions of the key in relation to the
            // data
            keyColumnPositions = getColumnsPosition(columns, keyColumns);
            keyColumnPositionsSet = true;
        }

        // now generate a key from the row of data...
        for (int keyColumnPosition : keyColumnPositions) {
            Object o = row.get(keyColumnPosition);

            if (o != null)
                key = key + o.toString() + "+";
            else
                key = null;
        }

        return key;
    }

    /**
     * return an int[] of the positions of the keyColumns, in the columns[]
     *
     * @param columns    column names
     * @param keyColumns key columns
     * @return int[]      positions of key columns in the column[]
     */
    public static List<Integer> getColumnsPosition(String[] columns, String[] keyColumns) {
        // first time in, set the positions of the key in relation to the data
        ArrayList<Integer> positions = new ArrayList<Integer>();
        for (String key : keyColumns) {
            for (int j = 0; j < columns.length; j++) {
                if (key.equalsIgnoreCase(columns[j].trim())) {
                    positions.add(j);
                    break;
                }
            }
        }
        return positions;
    }

    /**
     * Given a key and java.sql.ResultSet, process the data and set the array of
     * columns, and also return a HashMap of data, keyed on the unique key
     * against an ArrayList representing a row.
     *
     * @param key  unique key
     * @param rs   result set
     * @param prop properties to use.
     * @return HashMap  map of keys to data
     * @throws Exception in case of any problems.
     */
    public HashMap processResultSet(String key, ResultSet rs, Properties prop) throws Exception {
        HashMap data = new HashMap();
        int count = 0;
        int[] compareColumnPosition = null;

        boolean handleNullsAsZero = valueOf(prop.getProperty("handleNullsAsZero"));
        boolean aggregate = valueOf(prop.getProperty("aggregate"));

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        String[] columns = getColumnsData(meta);
        String[] columnsClassNames = getColumnsClassNameData(meta);

        this.columns = columns;

        String[] keyColumns = convertStringKeyToArray(key,
                prop.getProperty("aggregate", " "));

        if (keysPresentInColumns(keyColumns, columns)) {
            // the key columns match with the meta data in the ResultSet so
            // proceed...

            // if we're aggregating, get the names of the compare columns to
            // bucket
            if (aggregate)
                compareColumnPosition = getCompareColumnsPosition(this.columns,
                        keyColumns);

            while (rs.next()) {
                ArrayList row = new ArrayList();

                for (int i = 0; i < columnCount; i++) {
                    Object o = rs.getObject(i + 1);

                    // for doubles which are null, and handleNullsAsZero is true
                    // default the value to 0.0
                    if (o == null && columnsClassNames[i].equals("java.lang.Double") && handleNullsAsZero) {
                        row.add(0.0);
                    } else {
                        try {
                            // if its double of float, try and limit the dp by
                            // using the pattern
                            // specified in the properties file
                            if (columnsClassNames[i].equals("java.lang.Double"))
                                o = new Double(
                                        (Recxx.m_dpFormatter.format(((Double) o)
                                                .doubleValue())));
                            else if (columnsClassNames[i]
                                    .equals("java.lang.Float"))
                                o = new Float(
                                        (Recxx.m_dpFormatter.format(((Float) o)
                                                .floatValue())));
                        } catch (NumberFormatException nfe) {
                            o = 0d;
                        }

                        // then add the row
                        row.add(o);
                    }
                }

                // try and save memory by trimming the arraylist to size
                row.trimToSize();

                String mapKey = generateKey(columns, keyColumns, row);

                if (mapKey != null) {
                    if (!data.containsKey(mapKey)) {
                        data.put(mapKey, row);
                    } else {
                        if (aggregate)
                            aggregateData(data, compareColumnPosition, row,
                                    mapKey);
                        else
                            LOGGER.log(Level.WARNING, "Key of "
                                    + key
                                    + " is not unique (duplicate values found for "
                                    + mapKey
                                    + ") - unless aggregation is specified, the rec wont work!");
                    }
                } else {
                    LOGGER.log(Level.WARNING, "Null key returned - discarding row");
                }

                count++;

                if (count % 1000 == 0)
                    LOGGER.log(Level.WARNING, "Loaded " + decimalFormatter.format(count)
                            + " (aggregated "
                            + decimalFormatter.format(data.size()) + ") row(s)");
            }
        } else {
            throw new Exception("Specified key " + key
                    + " not present in ResultSetMetaData");
        }

        LOGGER.log(Level.WARNING, "Loaded " + decimalFormatter.format(count) + " (aggregated "
                + decimalFormatter.format(data.size()) + ") row(s) in total");

        return data;
    }

    /**
     * given the data and the keys, aggregate the data
     *
     * @param data
     * @param compareColumnPosition
     * @param row
     * @param mapKey
     */
    public void aggregateData(HashMap data, int[] compareColumnPosition,
                              ArrayList row, String mapKey) throws Exception {
        ArrayList existingRow = (ArrayList) data.get(mapKey);

        try {
            for (int i = 0; i < compareColumnPosition.length; i++) {
                double aggregatedValue = (Double) existingRow
                        .get(compareColumnPosition[i])
                        + (Double) row.get(compareColumnPosition[i]);
                existingRow.set(compareColumnPosition[i], aggregatedValue);
            }
        } catch (ClassCastException cse) {
            LOGGER.log(Level.WARNING, cse.getMessage(), cse);
            throw new Exception(
                    "Unable to aggregate data as of 1 of the columns specified for comparision is not a numeric!");
        } catch (IndexOutOfBoundsException ie) {
            ie.printStackTrace();
        }
    }

    /**
     * return the position of the compare columns. By definition everything that
     * isn't a key column is a compare column to make it easy to setup big
     * queries. Of course, this means that the columns to compare must be in the
     * same order
     *
     * @param columns    columns used for compare
     * @param keyColumns columns used to match records.
     * @return Integer[]
     */
    public int[] getCompareColumnsPosition(String[] columns, String[] keyColumns) {
        // get the location of the keys
        List<Integer> keyPositions = getColumnsPosition(columns, keyColumns);

        // now work out the location of the columns to compare by assuming
        // everything that isn't a key
        // is a column to compare
        int[] comparePositions = new int[columns.length - keyPositions.size()];
        int count = 0;

        for (int i = 0; i < columns.length; i++) {
            boolean columnFound = false;
            for (int keyPosition : keyPositions) {
                if (i == keyPosition) {
                    columnFound = true;
                    break;
                }
            }

            if (!columnFound) {
                comparePositions[count] = i;
                count++;
            }
        }

        return comparePositions;

    }
}
