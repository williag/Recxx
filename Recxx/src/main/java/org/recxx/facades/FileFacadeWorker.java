package org.recxx.facades;

import org.recxx.AbstractRecFeed;
import org.recxx.Recxx;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents a facade on to a file for use as a data source when
 * reconciling two sources of data
 */
public class FileFacadeWorker extends AbstractRecFeed implements RecxxWorker {
    private Recxx m_Rec = null;
    private String m_ColumnNames = "";
    private static SimpleDateFormat m_Dtf = new SimpleDateFormat();

    private int[] m_KeyPositions;
    private int[] m_ComparePositions;
    private String[] m_KeyColumns;
    private String[] m_CompareColumns;
    private String[] m_ReducedColumns;

    protected Properties m_Properties = new Properties();

    Logger LOGGER = Logger.getLogger(FileFacadeWorker.class.getName());

    /**
     * Constructor for DatabaseFacadeWorker.
     */
    public FileFacadeWorker(String prefix, String propertiesFile) {
        // constructor
        init(prefix, propertiesFile);
    }

    /**
     * load the data from the file
     *
     * @see Runnable#run()
     */
    public void run() {
        try {
            getData();

            // update the data back..
            HashMap finishedData = new HashMap();
            finishedData.put(Recxx.COLUMNS, m_ReducedColumns);
            finishedData.put(Recxx.DATA, data);
            finishedData.put(Recxx.PROPERTIES, m_Properties);

            m_Rec.setDataToCompare(finishedData,
                    m_Properties.getProperty("order"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Set the properties to be used for worker
     */
    public void setRunTimeProperties(Properties p) {
        m_Properties = p;

        m_ColumnNames = m_Properties.getProperty("columns");
    }

    /**
     * set the datastore to return the data to
     */
    public void setDataStore(Recxx rec) {
        m_Rec = rec;
    }

    /**
     * validate the properties, run the query and process the data
     */
    private void getData() throws Exception {
        String filePath = m_Properties.getProperty("filePath");
        String delimiter = m_Properties.getProperty("delimiter");
        String aggregate = m_Properties.getProperty("aggregate");
        String key = m_Properties.getProperty("key");

        if (filePath == null || delimiter == null || aggregate == null
                || key == null) {
            m_Properties.list(System.err);
            throw new Exception(
                    "One or properties could not be found to initialise class..properties set are: ");
        } else {
            BufferedReader br = openFile(filePath);
            data = processFile(key, br, m_Properties);

            closeFile(br);
        }

    }

    /**
     * close the file once finished with
     *
     * @param br
     */
    private void closeFile(BufferedReader br) {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * return a buffered reader reference to the file to be loaded
     *
     * @param filePath
     * @return a buffered reader reference
     */
    private BufferedReader openFile(String filePath) throws Exception {
        BufferedReader br = null;

        try {
            FileReader fr = new FileReader(filePath);
            br = new BufferedReader(fr);
            LOGGER.info("Found file " + filePath);
        } catch (FileNotFoundException fnfe) {
            LOGGER.log(Level.SEVERE, "FileNotFoundException. Message is "
                    + fnfe);
            throw new Exception(fnfe.getMessage());
        }
        return br;
    }

    /**
     * Given a key and java.sql.ResultSet, process the data and set the array of
     * columns, and also return a HashMap of data, keyed on the unique key
     * against an ArrayList representing a row.
     *
     * @param key
     * @param prop
     * @return HashMap
     * @throws java.sql.SQLException
     * @throws Exception
     */
    public HashMap processFile(String key, BufferedReader br, Properties prop)
            throws SQLException, Exception {
        HashMap data = new HashMap();
        boolean fileEmpty = false;
        int count = 0;
        int[] compareColumnPosition = null;

        boolean handleNullsAsZero = (Boolean.valueOf(prop
                .getProperty("handleNullsAsZero"))).booleanValue();
        boolean aggregate = (Boolean.valueOf(prop.getProperty("aggregate")))
                .booleanValue();

        getColumnCount(br);

        String[] columns = getColumnsData(m_ColumnNames);
        String[] columnsClassNames = getColumnsClassNameData();

        columns = columns;

        m_KeyColumns = convertStringKeyToArray(key,
                m_Properties.getProperty("delimiter", " "));
        m_CompareColumns = convertStringKeyToArray(
                m_Properties.getProperty("columnsToCompare"),
                m_Properties.getProperty("delimiter", " "));
        // m_ReducedColumns = addArrays(m_KeyColumns,m_CompareColumns);
        m_ReducedColumns = addArraysProperly(columns, m_KeyColumns,
                m_CompareColumns);

        // set the positions of the key and the columns to compare....anything
        // else
        // will not be looked at...
        m_KeyPositions = getColumnsPosition(m_ReducedColumns, m_KeyColumns);
        m_ComparePositions = getColumnsPosition(m_ReducedColumns, m_CompareColumns);

        if (checkKeyColumns(m_KeyColumns, columns)) {
            // the key columns match with the meta data in the ResultSet so
            // proceed...

            // if we're aggregating, get the names of the compare columns to
            // bucket
            if (aggregate)
                compareColumnPosition = getCompareColumnsPosition(
                        m_ReducedColumns, m_KeyColumns);

            // check if the stream is empty yet
            while (!fileEmpty) {
                String line = br.readLine();

                if (line != null) {
                    ArrayList row = new ArrayList();
                    int columnCounter = 0;
                    StringTokenizer st = new StringTokenizer(correctLine(line),
                            m_Properties.getProperty("delimiter", " "));

                    while (st.hasMoreTokens()) {
                        // System.err.println(st.countTokens());

                        if (isAColumnToCompare(columnCounter, columns)) {
                            // cast the object to the correct datatype
                            Object o = castObject((Object) st.nextToken(),
                                    columnsClassNames[columnCounter]);

                            // for doubles which are null, and handleNullsAsZero
                            // is true
                            // default the value to 0.0
                            if (o == null
                                    && columnsClassNames[columnCounter]
                                    .equals("java.lang.Double")
                                    && handleNullsAsZero)
                                row.add(new Double(0.0));
                            else
                                row.add(o);
                        } else
                            st.nextToken();

                        columnCounter++;
                    }

                    // try and save memory by trimming the arraylist to size
                    row.trimToSize();

                    String mapKey = generateKey(m_ReducedColumns, m_KeyColumns,
                            row);

                    if (mapKey != null) {
                        if (!data.containsKey(mapKey)) {
                            data.put(mapKey, row);
                        } else {
                            if (aggregate)
                                aggregateData(data, compareColumnPosition, row,
                                        mapKey);

                            else
                                LOGGER.warning("Key of "
                                        + key
                                        + " is not unique (duplicate values found for "
                                        + mapKey
                                        + ") - unless aggregation is specified, the rec wont work!");
                        }
                    } else {
                        LOGGER.warning("Null key returned - discarding row");
                    }

                    count++;

                    if (count % 1000 == 0)
                        LOGGER.info("Loaded " + decimalFormatter.format(count)
                                + " (aggregated "
                                + decimalFormatter.format(data.size()) + ") row(s)");
                } else {
                    fileEmpty = true;
                }

            }

        } else {
            throw new Exception("Specified key " + key
                    + " not present in File columns data");
        }

        LOGGER.info("Loaded " + decimalFormatter.format(count) + " (aggregated "
                + decimalFormatter.format(data.size()) + ") row(s) in total");

        return data;
    }

    /**
     * As we only take the key and compare columns in the file facade, we need
     * to make sure they are in the right order..ie the order in which they are
     * in the file, not the order in which they are in the key and compare
     * properties...
     *
     * @param columns        - full set of columns
     * @param keyColumns     - key columns
     * @param compareColumns - comapre columns
     * @return
     */
    private String[] addArraysProperly(String[] columns, String[] keyColumns,
                                       String[] compareColumns) {
        String[] reducedColumns = new String[keyColumns.length
                + compareColumns.length];
        int counter = 0;
        int prevCounter = 0;

        // the files columns
        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < keyColumns.length; j++) {
                if (columns[i].equals(keyColumns[j])) {
                    reducedColumns[counter] = columns[i];
                    counter++;
                    break;
                }
            }

            if (reducedColumns[prevCounter] == null) {
                // then we haven't found the column yet
                for (int k = 0; k < compareColumns.length; k++) {
                    if (columns[i].equals(compareColumns[k])) {
                        reducedColumns[counter] = columns[i];
                        counter++;
                        break;
                    }
                }
            }

            prevCounter = counter;

            if (counter == reducedColumns.length)
                break;
        }

        return reducedColumns;
    }

    /**
     * Given a delimited row, just make sure any empty 'columns' have a space in
     * there, instead of nothing and just 2 delimiters next to each other..
     *
     * @param line
     * @return String corrected line
     */
    private String correctLine(String line) {
        // look for 2 delimiters next to each other, with no space
        String delimiter = m_Properties.getProperty("delimiter");

        while (line.indexOf(delimiter + delimiter) != -1) {
            int pos = line.indexOf(delimiter + delimiter);

            StringBuffer sb = new StringBuffer(line);
            sb.insert(pos + 1, "0");

            line = sb.toString();
        }

        if (m_Properties.getProperty("appendDelimiter").equals("true")) {
            // the line might have spaces in the last column, but have no final
            // delimiter, so add it here
            line = line + "0" + delimiter;
        }

        return line;
    }

    /**
     * return an array of column data types
     *
     * @return array of column data tyoes
     */
    private String[] getColumnsClassNameData() {
        StringTokenizer st = new StringTokenizer(
                m_Properties.getProperty("columnDataTypes"));

        String[] dataTypes = new String[st.countTokens()];
        int count = 0;

        while (st.hasMoreTokens()) {
            dataTypes[count] = st.nextToken();
            count++;
        }

        return dataTypes;
    }

    /**
     * get the column names
     *
     * @param columnNames
     * @return array of column names
     */
    private String[] getColumnsData(String columnNames) {
        StringTokenizer st = new StringTokenizer(columnNames,
                m_Properties.getProperty("delimiter"));

        String[] columns = new String[st.countTokens()];
        int count = 0;

        while (st.hasMoreTokens()) {
            columns[count] = st.nextToken();
            count++;
        }

        return columns;
    }

    /**
     * given a reference to the file, return the number of columns
     *
     * @param br
     * @return the number of columns
     */
    private int getColumnCount(BufferedReader br) {
        if ((Boolean.valueOf(m_Properties.getProperty("columnsSupplied")))
                .booleanValue()) {
            // then the first row is a list of the columns
            try {
                String columns = br.readLine();
                m_ColumnNames = columns;

                if ((Boolean.valueOf(m_Properties
                        .getProperty("dataTypesSupplied"))).booleanValue()) {
                    // for data produced by using the yolus regression sink, the
                    // second row is always
                    // the data types of the 1st row columns..so ignore it if
                    // the property is set to true
                    br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        StringTokenizer st = new StringTokenizer(m_ColumnNames,
                m_Properties.getProperty("delimiter"));
        return st.countTokens();

    }

    /**
     * Given an object, and a property specified java datatype create a new
     * object accordingly..to allow Rec2Inputs.recData() to work..
     *
     * @param o
     * @param columnDataType
     * @return the newly cast oject
     */
    private Object castObject(Object o, String columnDataType) {
        if (columnDataType.equals("java.lang.Double"))
            return new Double(Recxx.m_dpFormatter.format(Double
                    .parseDouble((String) o)));

        else if (columnDataType.equals("java.lang.Integer"))
            return new Integer((String) o);

        else if (columnDataType.equals("java.lang.String"))
            return new String((String) o).trim();

        else if (columnDataType.equals("java.lang.Float"))
            return new Float(Recxx.m_dpFormatter.format(Float
                    .parseFloat((String) o)));

        else if (columnDataType.equals("java.lang.Boolean"))
            return new Boolean((String) o);

        else if (columnDataType.equals("java.util.Date")) {
            if (o.equals("0"))
                return null;

            m_Dtf.applyPattern(m_Properties.getProperty("dateFormat"));
            try {
                return m_Dtf.parse((String) o);
            } catch (ParseException pe) {
                LOGGER.log(Level.SEVERE, "Problem formating date " + (String) o
                        + " using pattern "
                        + m_Properties.getProperty("dateFormat"), pe);
            }

            return (String) o;
        } else {
            LOGGER.info("Specified data type " + columnDataType
                    + " not found..defaulting value to java.lang.String");
            return new String("");
        }

    }

    /**
     * is the specified column number a valid column to compare? ie a key or a
     * compare?
     *
     * @param columnNumber
     * @return
     */
    private boolean isAColumnToCompare(int columnNumber, String[] columns) {
        boolean validColumn = false;

        /*
           * //check the keys first for (int i=0; i<m_KeyPositions.length;i++) {
           * if (m_KeyPositions[i] == columnNumber) { validColumn = true; break; }
           * }
           *
           * if (!validColumn) { //then check the compare columns for (int j=0;
           * j<m_ComparePositions.length;j++) { if (m_ComparePositions[j] ==
           * columnNumber) { validColumn = true; break; } } }
           */
        String columnName = columns[columnNumber];

        for (int i = 0; i < m_KeyColumns.length; i++) {
            if (m_KeyColumns[i].equals(columnName)) {
                validColumn = true;
                break;
            }
        }

        if (!validColumn) {
            // then check the compare columns
            for (int j = 0; j < m_CompareColumns.length; j++) {
                if (m_CompareColumns[j].equals(columnName)) {
                    validColumn = true;
                    break;
                }
            }
        }

        return validColumn;
    }

    /**
     * must be an easier way...? my brain has stopped..
     *
     * @param array1
     * @param array2
     * @return the full string array
     */
    private String[] addArrays(String[] array1, String[] array2) {
        String[] finalArray = new String[array1.length + array2.length];

        for (int i = 0; i < array1.length; i++) {
            finalArray[i] = array1[i];
        }
        for (int j = 0; j < array2.length; j++) {
            finalArray[array1.length + j] = array2[j];
        }

        return finalArray;
    }

}
