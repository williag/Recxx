package org.recxx;

import org.recxx.facades.DatabaseFacadeWorker;
import org.recxx.facades.FileFacadeWorker;
import org.recxx.facades.RecxxWorker;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * Generic SQL based reconciliation tool to allow comparison between data
 * sources which support the JDBC/SQL protocols, as well as files and/or from a
 * database and additionally, from delimited files. This tool currently only
 * supports comparison between 2 data sets,
 * <p/>
 * Properties for the 2 data sources are specified, and then the data sources
 * are loaded in seperate threads and placed into keyed HashMaps to allow
 * comparison. When both data sets are loaded, the reconciliation process then
 * takes place. The sets of sql specified for each data source must have the
 * columns for comparison in the same order, even if their names are different -
 * every column not specified in the 'key' (see below) is compared. An
 * aggregation property can also be specified, to aggregate numerical columns in
 * the data with the same key. This is of most use for aggregating File data, as
 * 'GROUP BY' can be used in the database sql instead.
 * <p/>
 * If all columns in the 2 rows match each other, then that row is considered as
 * matched. If any column between the 2 rows doesn't match, then the whole row
 * is considered as not matched.
 * <p/>
 * Differences can be logged in 2 different ways. Either simply log the
 * difference to System.err, however this can degrade performance for large
 * volumes of data. An alternative is to turn on CSV logging (see below) which
 * produces a comma delimited file of the differences.
 * <p/>
 * <p/>
 * The properties that are needed to use this class are listed below:
 * <p/>
 * Data source independent properties
 * <p/>
 * <ul>
 * <li>*.rec.toleranceLevel = numeric value, as a percentage, which sets the
 * absolute difference to be allowed when comparing numeric values, and still
 * allow that column to be classed as matched</li>
 * <li>*.rec.smallestAbsoluteValue = numeric value. If <b>both</b> abs(values)
 * are smaller than this, then they are classed as being compared successfully</li>
 * <li>*.rec.handleNullsAsDefault = if true, numeric columns which have null
 * values, are defaulted to 0.0</li>
 * <li>*.rec.outputType = defaults to 'csv', which allows logging to csv file.
 * Else, if set to 'err' logs to System.err</li>
 * <li>*.rec.logger.csv.file = if outputType set to 'csv', this property needs
 * to be set, to specify the location of the csv file</li>
 * </ul>
 * <p/>
 * Database properties
 * <p/>
 * <ul>
 * <li>*.rec.inputSource<i>n</i>.db.uid = Database user id</li>
 * <li>*.rec.inputSource<i>n</i>.db.pwd = Database password</li>
 * <li>*.rec.inputSource<i>n</i>.db.jdbc.url = Database JDBC url</li>
 * <li>*.rec.inputSource<i>n</i>.db.jdbc.driver = JDBC driver to use to connect
 * to the database</li>
 * <li>*.rec.inputSource<i>n</i>.db.sql = SQL to run on the database</li>
 * <li>*.rec.inputSource<i>n</i>.db.key = Unique key for data</li>
 * </ul>
 * <p/>
 * File properties
 * <p/>
 * <ul>
 * <li>*.rec.inputSource<i>n</i>.file.filePath = location of the file to load</li>
 * <li>*.rec.inputSource<i>n</i>.file.delimiter = delimiter delimiting the
 * columns</li>
 * <li>*.rec.inputSource<i>n</i>.file.firstRowColumns = is the first row of the
 * file column headings?</li>
 * <li>*.rec.inputSource<i>n</i>.file.Columns = if firstRowColumns=false, then
 * this must be added with all the column names</li>
 * <li>*.rec.inputSource<i>n</i>.file.columnDataTypes = java data types for
 * <i>all</i> the columns (java.lang.String, java.lang.Double etc)</li>
 * <li>*.rec.inputSource<i>n</i>.file.columnDataTypes.date.format= Pattern to
 * allow any date strings to be converted to java.util.Date objects...ie
 * yyyyMMdd</li>
 * <li>*.rec.inputSource<i>n</i>.file.key = Unique key for data</li>
 * <li>*.rec.inputSource<i>n</i>.file.columnsToCompare = In place of sql, the
 * columns that are to be reconciled</li>
 * <li>*.rec.inputSource<i>n</i>.file.aggregate = if true, aggregates data rows
 * with the same key, for the compare columns</li>
 * </ul>
 */
public class Recxx extends AbstractRecFeed implements Runnable {

    private static final String m_appName = "rec";
    public static final String DB_INPUT = "DB";
    public static final String FILE_INPUT = "File";
    public static final String COLUMNS = "Columns";
    public static final String DATA = "Data";
    public static final String PROPERTIES = "Props";

    private String FILE_LOCATION;
    private String FILE_DELIMITER;

    // default reconciliation process it Two-Way (TW) as opposed to One-Way (OW)
    private String m_recMode = "TW";
    protected String m_delimiter = " ";

    private String m_outputType = "";
    private boolean m_loggerInit = false;
    private CSVLogger m_logger;
    private DecimalFormat m_dPercentageFormatter = new DecimalFormat("#.00%");
    public static DecimalFormat m_dpFormatter;

    protected HashMap m_propertiesMap;
    protected HashMap m_dataToCompare = new HashMap();
    protected int m_dataToCompareKey = 0;
    protected ThreadGroup m_workerGroup = new ThreadGroup("Worker Group");

    Logger LOGGER = Logger.getLogger(Recxx.class.getName());

    /**
     * Constructor for Rec2Inputs.
     *
     * @param args
     */
    public Recxx(String[] args) {
        super();
        init(args[0], args[1]);
    }

    /**
     * Normal main method to start up the class from a command line
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage: " + Recxx.class.getName()
                    + " <prefix> <properties file>\n" + "Example: "
                    + Recxx.class.getName() + " \\properties\\system.properties");
        }

        Recxx rec = new Recxx(args);

        Thread t = new Thread(rec);
        t.start();
    }

    /**
     * run the class
     *
     * @see java.lang.Runnable#run()
     */
    public void run() {

        try {
            // firstly load up the properties....
            loadProperties();

            // then load the data sources in separate threads....
            startThreads();

            // now wait for the threads to finish
            waitForThreads();

            // now rec the data calling the correct method according to the mode
            if (m_recMode.equalsIgnoreCase("TW"))
                recData();
            else
                oldRecData();

            // tidy up any connections etc
            close();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    /**
     * close the csv logger, if open.
     *
     * @throws java.io.IOException
     */
    private void close() throws IOException {
        if (m_outputType.equals("csv") && m_logger != null)
            m_logger.close();
    }

    /**
     * Method recData.
     *
     * @throws Exception
     */
    private void recData() throws Exception {

        String[] inputColumns1, inputColumns2;

        HashMap inputData1, inputData2;

        int inputData1Size, inputData2Size;

        Properties inputProperties1, inputProperties2;

        String input1Alias, input2Alias;

        int input1MatchedRows = 0;
        float tolerancePercentage, smallestAbsoluteValue;

        LOGGER.info("Starting to reconcile data sources...");

        if (m_dataToCompare.size() >= 2) {
            inputColumns1 = (String[]) ((HashMap) m_dataToCompare
                    .get("1")).get(COLUMNS);
            inputData1 = (HashMap) ((HashMap) m_dataToCompare.get("1")).get(DATA);
            inputProperties1 = (Properties) ((HashMap) m_dataToCompare
                    .get("1")).get(PROPERTIES);
            inputData1Size = inputData1.size();

            inputColumns2 = (String[]) ((HashMap) m_dataToCompare
                    .get("2")).get(COLUMNS);
            inputData2 = (HashMap) ((HashMap) m_dataToCompare.get("2")).get(DATA);
            inputProperties2 = (Properties) ((HashMap) m_dataToCompare
                    .get("2")).get(PROPERTIES);
            inputData2Size = inputData2.size();

            // need a position of the compare columns in the array - do this by
            // making every column which isn't a
            // key column, a compare column
            int[] input1CompareColumnPosition = getCompareColumnsPosition(
                    inputColumns1,
                    convertStringKeyToArray(
                            (String) inputProperties1.get("key"), m_delimiter));
            int[] input2CompareColumnPosition = getCompareColumnsPosition(
                    inputColumns2,
                    convertStringKeyToArray(
                            (String) inputProperties2.get("key"), m_delimiter));

            input1Alias = (String) inputProperties1.get("alias");
            input2Alias = (String) inputProperties2.get("alias");

            if (input1CompareColumnPosition.length != input2CompareColumnPosition.length)
                throw new Exception("Unequal number of columns to compare - "
                        + input1CompareColumnPosition.length + " vs "
                        + input2CompareColumnPosition.length);

            // now set the tolerance level as a percentage
            tolerancePercentage = Float.parseFloat(((String) inputProperties1
                    .get("tolerance")));
            smallestAbsoluteValue = Float.parseFloat(((String) inputProperties1
                    .get("smallestAbsoluteValue")));

            Iterator inputIterator = inputData1.keySet().iterator();

            LOGGER.info("Comparing "
                    + decimalFormatter.format(inputData1.size()) + " rows from "
                    + input1Alias + " with "
                    + decimalFormatter.format(inputData2.size()) + " rows from "
                    + input2Alias + " over "
                    + input1CompareColumnPosition.length + " column(s)");

            while (inputIterator.hasNext()) {
                String key = (String) inputIterator.next();

                boolean matchedRow = true;
                boolean unhandledRow = false;

                if (inputData2.containsKey(key)) {
                    // loop round the input1 columns to compare - a row is only
                    // deemed as matched
                    // if _all_ the columns selected to compare, match..
                    for (int i = 0; i < input1CompareColumnPosition.length; i++) {
                        Object o1 = ((ArrayList) inputData1.get(key))
                                .get(input1CompareColumnPosition[i]);
                        Object o2 = ((ArrayList) inputData2.get(key))
                                .get(input2CompareColumnPosition[i]);

                        if (o1 instanceof Double && o2 instanceof Double) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            if (Math.abs(((Double) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((Double) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((Double) o1).doubleValue() - ((Double) o2)
                                                .doubleValue()) / ((Double) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((Double) o1)
                                            .doubleValue()
                                            - ((Double) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof BigDecimal
                                && o2 instanceof Double) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            // NSB - 16/6/04 - Added as Oracle returns Big
                            // Decimals
                            if (Math.abs(((BigDecimal) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((Double) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((BigDecimal) o1).doubleValue() - ((Double) o2)
                                                .doubleValue()) / ((BigDecimal) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((BigDecimal) o1)
                                            .doubleValue()
                                            - ((Double) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof Double
                                && o2 instanceof BigDecimal) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            // NSB - 16/6/04 - Added as Oracle returns Big
                            // Decimals
                            if (Math.abs(((Double) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((BigDecimal) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((Double) o1).doubleValue() - ((BigDecimal) o2)
                                                .doubleValue()) / ((Double) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((Double) o1)
                                            .doubleValue()
                                            - ((BigDecimal) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof Integer
                                && o2 instanceof Integer) {
                            try {
                                // only look at rows greater than the absolute
                                // smallest value specified
                                if (Math.abs(((Integer) o1).intValue()) > smallestAbsoluteValue
                                        && Math.abs(((Integer) o2).intValue()) > smallestAbsoluteValue) {
                                    int percentageDiff = Math
                                            .abs(((((Integer) o1).intValue() - ((Integer) o2)
                                                    .intValue()) / ((Integer) o1)
                                                    .intValue()) * 100);
                                    if (percentageDiff > tolerancePercentage) {
                                        int absDiff = Math.abs(((Integer) o1)
                                                .intValue()
                                                - ((Integer) o2).intValue());
                                        logDifference(
                                                (String) inputProperties1
                                                        .get("key"),
                                                key,
                                                input1Alias,
                                                inputColumns1[input1CompareColumnPosition[i]],
                                                o1,
                                                input2Alias,
                                                inputColumns2[input2CompareColumnPosition[i]],
                                                o2,
                                                String.valueOf(percentageDiff),
                                                String.valueOf(absDiff));
                                        matchedRow = false;
                                    }
                                }
                            } catch (ArithmeticException ae) {
                                if (!((Integer) o1).equals(o2)) {
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, "", "");
                                    matchedRow = false;
                                }

                            }
                        } else if (o1 instanceof String && o2 instanceof String) {
                            if (!(((String) o1).equals((String) o2))) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 instanceof Boolean
                                && o2 instanceof Boolean) {

                            if (!((Boolean) o1).equals(o2)) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 instanceof java.util.Date
                                && o2 instanceof java.util.Date) {
                            if (!(((java.util.Date) o1)
                                    .equals((java.util.Date) o2))) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 == null || o2 == null) {
                            if (o1 == null && o2 == null) {
                                // do nothing
                            } else {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else {
                            LOGGER.severe("Either encountered 2 different data types, or un-handled data type!");
                            LOGGER.severe("O1= " + o1.getClass().getName() + ", O2= " + o2.getClass().getName());
                            unhandledRow = true;
                        }
                    }

                    if (matchedRow)
                        input1MatchedRows++;

                    // At this point we have:
                    // - Found a matching row
                    // - Checked all the columns which needed to be checked
                    //
                    // Provide that 'unhandledRow' is not true we can remove
                    // this row entry from inputData1 & inputData2
                    if (!unhandledRow) {
                        // remove from inputData1
                        inputIterator.remove();
                        // remove from inputData2
                        inputData2.remove(key);
                    }
                }
            }

            // At this point the data in inputData1 & inputData2 are unmatched
            // items only. ie enteries which are in one
            // data set and not in the other.
            //
            // Now we just need to traverse each in turn and display them
            inputIterator = inputData1.keySet().iterator();
            while (inputIterator.hasNext()) {
                String key = (String) inputIterator.next();
                // for keys that are missing,show all the values that are
                // actualy there, vs 'Missing'
                for (int j = 0; j < input1CompareColumnPosition.length; j++) {
                    Object o1 = (Object) ((ArrayList) inputData1.get(key))
                            .get(input1CompareColumnPosition[j]);

                    if ((o1 instanceof Double || o1 instanceof Integer || o1 instanceof String)) {
                        // only log a difference here, if o1 is <> 0.0, even if
                        // 02 is actually missing..
                        logDifference((String) inputProperties1.get("key"),
                                key, input1Alias,
                                inputColumns1[input1CompareColumnPosition[j]],
                                o1, input2Alias, "Missing", "Missing", "", "");
                    }
                }
            }
            inputIterator = inputData2.keySet().iterator();
            while (inputIterator.hasNext()) {
                String key = (String) inputIterator.next();
                // for keys that are missing,show all the values that are
                // actualy there, vs 'Missing'
                for (int j = 0; j < input2CompareColumnPosition.length; j++) {
                    Object o1 = (Object) ((ArrayList) inputData2.get(key))
                            .get(input2CompareColumnPosition[j]);

                    if ((o1 instanceof Double || o1 instanceof Integer || o1 instanceof String)) {
                        // only log a difference here, if o1 is <> 0.0, even if
                        // 02 is actually missing..
                        logDifference((String) inputProperties2.get("key"),
                                key, input2Alias, "Missing", "Missing",
                                input1Alias,
                                inputColumns2[input2CompareColumnPosition[j]],
                                o1, "", "");
                    }
                }
            }
        } else {
            throw new Exception(
                    "A reconciliation requires 2 or more data inputs - current data inputs size is "
                            + m_dataToCompare.size());
        }

        logSummary(input1Alias, inputData1Size, input2Alias, inputData2Size,
                input1MatchedRows);
    }

    /**
     * Method recData.
     *
     * @throws Exception
     */
    private void oldRecData() throws Exception {

        String[] inputColumns1 = null;
        String[] inputColumns2 = null;

        HashMap inputData1 = null;
        HashMap inputData2 = null;

        Properties inputProperties1 = null;
        Properties inputProperties2 = null;

        String input1Alias = null;
        String input2Alias = null;

        int input1MatchedRows = 0;
        float tolerancePercentage = 0.0f;
        float smallestAbsoluteValue = 0.0f;

        LOGGER.info("Starting to reconcile data sources...");

        if (m_dataToCompare.size() >= 2) {
            inputColumns1 = (String[]) ((HashMap) m_dataToCompare
                    .get("1")).get(COLUMNS);
            inputData1 = (HashMap) ((HashMap) m_dataToCompare.get(
                    "1")).get(DATA);
            inputProperties1 = (Properties) ((HashMap) m_dataToCompare
                    .get("1")).get(PROPERTIES);

            inputColumns2 = (String[]) ((HashMap) m_dataToCompare
                    .get("2")).get(COLUMNS);
            inputData2 = (HashMap) ((HashMap) m_dataToCompare.get(
                    "2")).get(DATA);
            inputProperties2 = (Properties) ((HashMap) m_dataToCompare
                    .get("2")).get(PROPERTIES);

            // need a position of the compare columns in the array - do this by
            // making every column which isn't a
            // key column, a compare column
            int[] input1CompareColumnPosition = getCompareColumnsPosition(
                    inputColumns1,
                    convertStringKeyToArray(
                            (String) inputProperties1.get("key"), m_delimiter));
            int[] input2CompareColumnPosition = getCompareColumnsPosition(
                    inputColumns2,
                    convertStringKeyToArray(
                            (String) inputProperties2.get("key"), m_delimiter));

            input1Alias = (String) inputProperties1.get("alias");
            input2Alias = (String) inputProperties2.get("alias");

            if (input1CompareColumnPosition.length != input2CompareColumnPosition.length)
                throw new Exception("Unequal number of columns to compare - "
                        + input1CompareColumnPosition.length + " vs "
                        + input2CompareColumnPosition.length);

            // now set the tolerance level as a percentage
            tolerancePercentage = Float.parseFloat(((String) inputProperties1
                    .get("tolerance")));
            smallestAbsoluteValue = Float.parseFloat(((String) inputProperties1
                    .get("smallestAbsoluteValue")));

            Iterator inputIterator = inputData1.keySet().iterator();

            LOGGER.info("Comparing "
                    + decimalFormatter.format(inputData1.size()) + " rows from "
                    + input1Alias + " with "
                    + decimalFormatter.format(inputData2.size()) + " rows from "
                    + input2Alias + " over "
                    + input1CompareColumnPosition.length + " column(s)");

            while (inputIterator.hasNext()) {
                String key = (String) inputIterator.next();

                boolean matchedRow = true;

                if (inputData2.containsKey(key)) {
                    // loop round the input1 columns to compare - a row is only
                    // deemed as matched
                    // if _all_ the columns selected to compare, match..
                    for (int i = 0; i < input1CompareColumnPosition.length; i++) {
                        Object o1 = (Object) ((ArrayList) inputData1.get(key))
                                .get(input1CompareColumnPosition[i]);
                        Object o2 = (Object) ((ArrayList) inputData2.get(key))
                                .get(input2CompareColumnPosition[i]);

                        if (o1 instanceof Double && o2 instanceof Double) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            if (Math.abs(((Double) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((Double) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((Double) o1).doubleValue() - ((Double) o2)
                                                .doubleValue()) / ((Double) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((Double) o1)
                                            .doubleValue()
                                            - ((Double) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof BigDecimal
                                && o2 instanceof Double) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            // NSB - 16/6/04 - Added as Oracle returns Big
                            // Decimals
                            if (Math.abs(((BigDecimal) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((Double) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((BigDecimal) o1).doubleValue() - ((Double) o2)
                                                .doubleValue()) / ((BigDecimal) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((BigDecimal) o1)
                                            .doubleValue()
                                            - ((Double) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof Double
                                && o2 instanceof BigDecimal) {
                            // only look at rows greater than the absolute
                            // smallest value specified
                            // NSB - 16/6/04 - Added as Oracle returns Big
                            // Decimals
                            if (Math.abs(((Double) o1).doubleValue()) > smallestAbsoluteValue
                                    && Math.abs(((BigDecimal) o2).doubleValue()) > smallestAbsoluteValue) {
                                double percentageDiff = Math
                                        .abs(((((Double) o1).doubleValue() - ((BigDecimal) o2)
                                                .doubleValue()) / ((Double) o1)
                                                .doubleValue()) * 100);
                                if (percentageDiff > tolerancePercentage) {
                                    double absDiff = Math.abs(((Double) o1)
                                            .doubleValue()
                                            - ((BigDecimal) o2).doubleValue());
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, String.valueOf(percentageDiff),
                                            String.valueOf(absDiff));
                                    matchedRow = false;
                                }
                            }
                        } else if (o1 instanceof Integer
                                && o2 instanceof Integer) {
                            try {
                                // only look at rows greater than the absolute
                                // smallest value specified
                                if (Math.abs(((Integer) o1).intValue()) > smallestAbsoluteValue
                                        && Math.abs(((Integer) o2).intValue()) > smallestAbsoluteValue) {
                                    int percentageDiff = Math
                                            .abs(((((Integer) o1).intValue() - ((Integer) o2)
                                                    .intValue()) / ((Integer) o1)
                                                    .intValue()) * 100);
                                    if (percentageDiff > tolerancePercentage) {
                                        int absDiff = Math.abs(((Integer) o1)
                                                .intValue()
                                                - ((Integer) o2).intValue());
                                        logDifference(
                                                (String) inputProperties1
                                                        .get("key"),
                                                key,
                                                input1Alias,
                                                inputColumns1[input1CompareColumnPosition[i]],
                                                o1,
                                                input2Alias,
                                                inputColumns2[input2CompareColumnPosition[i]],
                                                o2,
                                                String.valueOf(percentageDiff),
                                                String.valueOf(absDiff));
                                        matchedRow = false;
                                    }
                                }
                            } catch (ArithmeticException ae) {
                                if (!((Integer) o1).equals(o2)) {
                                    logDifference(
                                            (String) inputProperties1
                                                    .get("key"),
                                            key,
                                            input1Alias,
                                            inputColumns1[input1CompareColumnPosition[i]],
                                            o1,
                                            input2Alias,
                                            inputColumns2[input2CompareColumnPosition[i]],
                                            o2, "", "");
                                    matchedRow = false;
                                }

                            }
                        } else if (o1 instanceof String && o2 instanceof String) {
                            if (!(((String) o1).equals((String) o2))) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 instanceof Boolean
                                && o2 instanceof Boolean) {

                            if (!((Boolean) o1).equals(o2)) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 instanceof java.util.Date
                                && o2 instanceof java.util.Date) {
                            if (!(((java.util.Date) o1)
                                    .equals((java.util.Date) o2))) {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else if (o1 == null || o2 == null) {
                            if (o1 == null && o2 == null) {
                                // do nothing
                            } else {
                                logDifference(
                                        (String) inputProperties1.get("key"),
                                        key,
                                        input1Alias,
                                        inputColumns1[input1CompareColumnPosition[i]],
                                        o1,
                                        input2Alias,
                                        inputColumns2[input2CompareColumnPosition[i]],
                                        o2, "", "");
                                matchedRow = false;
                            }
                        } else {
                            LOGGER.severe("Either encountered 2 different data types, or un-handled data type!");
                            LOGGER.severe("O1= " + o1.getClass().getName() + ", O2= " + o2.getClass().getName());
                        }
                    }

                } else {
                    // for keys that are missing,show all the values that are
                    // actualy there, vs 'Missing'
                    for (int j = 0; j < input1CompareColumnPosition.length; j++) {
                        Object o1 = (Object) ((ArrayList) inputData1.get(key))
                                .get(input1CompareColumnPosition[j]);

                        if ((o1 instanceof Double || o1 instanceof Integer || o1 instanceof String)) {
                            // only log a difference here, if o1 is <> 0.0, even
                            // if 02 is actually missing..
                            logDifference(
                                    (String) inputProperties1.get("key"),
                                    key,
                                    input1Alias,
                                    inputColumns1[input1CompareColumnPosition[j]],
                                    o1, input2Alias, "Missing", "Missing", "",
                                    "");
                            matchedRow = false;
                        }
                    }

                }

                if (matchedRow)
                    input1MatchedRows++;
            }
        } else {
            throw new Exception(
                    "A reconciliation requires 2 or more data inputs - current data inputs size is "
                            + m_dataToCompare.size());
        }

        logSummary(input1Alias, inputData1.size(), input2Alias,
                inputData2.size(), input1MatchedRows);
    }

    /**
     * wait for all the worker threads to finish....only returns after all have
     * finshed.
     */
    private void waitForThreads() throws InterruptedException {
        Thread[] threads = new Thread[m_workerGroup.activeCount()];

        m_workerGroup.enumerate(threads);

        if (threads != null) {
            for (int i = 0; i < threads.length; i++) {
                if (threads[i].isAlive())
                    threads[i].join();

                LOGGER.info("Thread " + threads[i].getName() + " finished");
            }
        }
    }

    /**
     * start up all the worker threads to start loading the data
     */
    private void startThreads() {
        // loop thru the sources and start them loading...
        Iterator sourceIterator = m_propertiesMap.keySet().iterator();

        while (sourceIterator.hasNext()) {
            String key = (String) sourceIterator.next();
            Properties sourceProperties = (Properties) m_propertiesMap
                    .get((key));

            String type = (String) sourceProperties.get("type");
            RecxxWorker worker = null;

            if (type.equals(DB_INPUT)) {
                worker = new DatabaseFacadeWorker(prefix, propertiesFile);
            } else if (type.equals(FILE_INPUT)) {
                worker = new FileFacadeWorker(prefix, propertiesFile);
            }
            if (worker != null) {
                worker.setRunTimeProperties(sourceProperties);
                worker.setDataStore(this);

                Thread t = new Thread(m_workerGroup, worker, key);
                t.start();
            }
        }
    }

    /**
     * load most of the properties in when the class initialises, to make the
     * log files ,look clearer
     */
    private void loadProperties() throws Exception {
        // load most of the parameters in at the beginning, as it looks neater
        // in the log files
        m_propertiesMap = new HashMap();
        Properties props = null;

        int numberOfInputs = 2;
        String tolerance = superProps.getProperty(prefix + "."
                + m_appName + ".toleranceLevel", "0.0");
        String handleNullsAsZero = superProps.getProperty(prefix
                + "." + m_appName + ".handleNullsAsDefault", "true");
        String smallestAbsoluteValue = superProps.getProperty(prefix
                + "." + m_appName + ".smallestAbsoluteValue", "0.0001");

        m_delimiter = superProps.getProperty(prefix + "." + m_appName
                + ".delimiter", " ");

        m_dpFormatter = new DecimalFormat(superProps.getProperty(
                prefix + "." + m_appName + ".decimalPlacesPattern",
                "#.00000000000"));

        m_outputType = superProps.getProperty(prefix + "."
                + m_appName + ".outputType", "csv");

        // get the recMode property determining whether the reconciliation is
        // one-way or two-way
        String recM = superProps.getProperty(prefix + "." + m_appName
                + ".reconciliationMode", "TW");
        if (recM.equalsIgnoreCase("TW")) {
            LOGGER.info("Performing two-way reconciliation...");
        } else if (recM.equalsIgnoreCase("OW")) {
            LOGGER.info("Performing one-way reconciliation...");
            m_recMode = "OW";
        } else {
            LOGGER.info("Unrecognised reconciliationMode entry: " + recM
                    + ", defaulting to performing a Two-way reconciliation...");
        }

        if (m_outputType.equals("csv")) {
            FILE_LOCATION = superProps.getProperty(prefix + "."
                    + m_appName + ".logger.csv.file");
            FILE_DELIMITER = superProps.getProperty(prefix + "."
                    + m_appName + ".logger.csv.file.delimiter", ",");
        }

        // TODO remove the redundancies here!!

        for (int i = 1; i <= numberOfInputs; i++) {
            String inputName = superProps.getProperty(prefix + "."
                    + m_appName + ".inputSource" + i + ".name.alias");
            String inputSource = superProps.getProperty(prefix + "."
                    + m_appName + ".inputSource" + i + ".name.type");

            props = new Properties();
            props.setProperty("alias", inputName);
            props.setProperty("type", inputSource);
            props.setProperty("tolerance", tolerance);
            props.setProperty("handleNullsAsZero", handleNullsAsZero);
            props.setProperty("smallestAbsoluteValue", smallestAbsoluteValue);
            props.setProperty("order", String.valueOf(i));
            props.setProperty("delimiter", m_delimiter);

            if (inputSource.equals(DB_INPUT)) {
                // Database source
                props.setProperty(
                        "uid",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i + ".db.uid"));
                props.setProperty(
                        "pwd",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i + ".db.pwd"));
                props.setProperty(
                        "url",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".db.jdbc.url"));
                props.setProperty(
                        "driver",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".db.jdbc.driver"));
                props.setProperty(
                        "sql",
                        loadStringPropertyFromFile(
                                superProps.getProperty(prefix + "."
                                        + m_appName + ".inputSource" + i
                                        + ".db.sql"), "select"));
                props.setProperty(
                        "key",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i + ".db.key"));
                props.setProperty("aggregate", new String("false"));

                m_propertiesMap.put(inputName, props);
            } else if (inputSource.equals(FILE_INPUT)) {
                // delimited file source
                props.setProperty(
                        "filePath",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.filePath"));
                props.setProperty(
                        "delimiter",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.delimiter", ","));
                props.setProperty(
                        "columnsSupplied",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.firstRowColumns", "true"));
                props.setProperty(
                        "dataTypesSupplied",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.secondRowDataTypes", "false"));

                if (!props.getProperty("columnsSupplied").equals("true")) {
                    // then the columns have to be specified in a property,
                    // seperated by spaces (just like the key)
                    props.setProperty(
                            "columns",
                            superProps.getProperty(prefix + "."
                                    + m_appName + ".inputSource" + i
                                    + ".file.columns"));
                }

                props.setProperty(
                        "columnDataTypes",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.columnDataTypes"));
                props.setProperty("dateFormat", superProps.getProperty(
                        prefix + "." + m_appName + ".inputSource" + i
                                + ".file.columnDataTypes.date.format",
                        "yyyyMMdd"));
                props.setProperty(
                        "key",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i + ".file.key"));
                props.setProperty(
                        "columnsToCompare",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.columnsToCompare"));
                props.setProperty(
                        "aggregate",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.aggregate", "false"));
                props.setProperty(
                        "appendDelimiter",
                        superProps.getProperty(prefix + "."
                                + m_appName + ".inputSource" + i
                                + ".file.appendDelimiter", "false"));

                m_propertiesMap.put(inputName, props);
            } else {
                throw new Exception("Invalid input source	" + inputSource
                        + " - can only be File or DB");
            }
        }

        if (m_propertiesMap.size() != numberOfInputs)
            throw new Exception(numberOfInputs + " were not loaded...!");
    }

    /**
     * if the propertyValue starts with the checkValue, ignoring the case of
     * each, then treat the propertyValue as a file path and load up from there,
     * otherwise just return the propertyValue
     *
     * @param propertyValue
     * @param checkValue
     * @return String
     * @throws Exception
     */
    private String loadStringPropertyFromFile(String propertyValue,
                                              String checkValue) throws Exception {

        if (!propertyValue.toLowerCase().startsWith(checkValue.toLowerCase())) {
            // then assume its a path to a file, so treat it as such
            FileReader fr = null;
            BufferedReader br = null;
            StringBuffer realPropertyValue = new StringBuffer();

            try {
                fr = new FileReader(propertyValue);
                br = new BufferedReader(fr);

                while (br.ready()) {
                    realPropertyValue.append(br.readLine());
                }
            } catch (FileNotFoundException fnfe) {
                LOGGER.severe("Property value file name " + propertyValue
                        + " could not be found");
                throw new Exception(fnfe.getMessage());
            } catch (IOException ioe) {
                LOGGER.severe("Problem reading from file " + propertyValue);
                throw new Exception(ioe.getMessage());
            } finally {
                // tidy up and close the file references
                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();
            }

            return realPropertyValue.toString();
        } else {
            // its the actual value we want, not a reference to a file which
            // contains the data
            // so just return the value
            return propertyValue;
        }
    }

    /**
     * Sets the dataToCompare.
     *
     * @param dataToCompare The dataToCompare to set
     */
    public synchronized void setDataToCompare(HashMap dataToCompare, String key) {
        m_dataToCompare.put(key, dataToCompare);
        m_dataToCompareKey++;
    }

    /**
     * Log a difference between the 2 data sets. Depending on m_outputType, the
     * difference is logged to System.err.or a specified csv file.
     *
     * @param keyColumns
     * @param key
     * @param alias1
     * @param columnName1
     * @param columnValue1
     * @param alias2
     * @param columnName2
     * @param columnValue2
     */
    private void logDifference(String keyColumns, String key, String alias1,
                               String columnName1, Object columnValue1, String alias2,
                               String columnName2, Object columnValue2, String percentageDiff,
                               String absDiff) {
        if (m_outputType.equals("csv")) {
            try {
                initCsvFile(alias1, alias2, keyColumns);
                logDifferenceToFile(key, alias1, columnName1, columnValue1,
                        alias2, columnName2, columnValue2, percentageDiff,
                        absDiff);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        } else if (m_outputType.equals("err")) {
            logDifferenceToSystemErr(key, alias1, columnName1, columnValue1,
                    alias2, columnName2, columnValue2);
        }
    }

    /**
     * log a difference to csv file
     *
     * @param key
     * @param alias1
     * @param columnName1
     * @param columnValue1
     * @param alias2
     * @param columnName2
     * @param columnValue2
     */
    private void logDifferenceToFile(String key, String alias1,
                                     String columnName1, Object columnValue1, String alias2,
                                     String columnName2, Object columnValue2, String percentageDiff,
                                     String absDiff) throws IOException {
        StringTokenizer st = new StringTokenizer(key, "+");

        while (st.hasMoreTokens()) {
            m_logger.write(st.nextToken());
        }

        m_logger.write(columnName1);

        if (columnValue1 != null)
            m_logger.write(columnValue1.toString());
        else
            m_logger.write("null");

        m_logger.write(columnName2);

        if (columnValue2 != null)
            m_logger.write(columnValue2.toString());
        else
            m_logger.write("null");

        m_logger.write(percentageDiff);
        m_logger.writeln(absDiff);

    }

    /**
     * log a summary report to file detailing rows matched etc etc
     *
     * @param alias1
     * @param rowCount1
     * @param alias2
     * @param rowCount2
     * @param rowsMatched
     * @throws IOException
     */
    private void logSummaryToFile(String alias1, int rowCount1, String alias2,
                                  int rowCount2, int rowsMatched) throws IOException {
        // 2 blank lines to seperate out the summary from the rest of the
        // results
        m_logger.writeln("");
        m_logger.writeln("");
        m_logger.writeln("=======================");
        m_logger.writeln("Reconciliation Report");
        m_logger.writeln("=======================");
        m_logger.write(alias1 + " rows");
        m_logger.writeln(rowCount1);
        m_logger.write(alias2 + " rows");
        m_logger.writeln(rowCount2);
        m_logger.write(alias1 + " matched to " + alias2);
        m_logger.writeln(rowsMatched);
        m_logger.write(alias1 + " matched to " + alias2 + " %");

        Integer i = new Integer(rowsMatched);
        Integer ii = new Integer(rowCount1);
        Integer ii2 = new Integer(rowCount2);

        m_logger.writeln(m_dPercentageFormatter.format(i.floatValue()
                / ii.floatValue()));
        m_logger.write(alias2 + " matched to " + alias1 + " %");
        m_logger.writeln(m_dPercentageFormatter.format(i.floatValue()
                / ii2.floatValue()));

        // loop thru the sources and start them loading...
        Iterator sourceIterator = m_propertiesMap.keySet().iterator();
        Properties props = new Properties();

        while (sourceIterator.hasNext()) {
            String key = (String) sourceIterator.next();
            props = (Properties) m_propertiesMap.get((key));
        }

        // at some point, put this in the equivalent of a toString() method
        // and loop out all the properties values but maybe not the passwords!
        m_logger.writeln("");
        m_logger.writeln("");
        m_logger.writeln("=======================");
        m_logger.writeln("Report Properties");
        m_logger.writeln("=======================");
        m_logger.write("Rec File Date/Time");
        m_logger.writeln(m_logger.toString());
        m_logger.write("Tolerance Level %");
        m_logger.writeln(props.getProperty("tolerance"));
        m_logger.write("HandleNullsAsZero?");
        m_logger.writeln(props.getProperty("handleNullsAsZero"));
        m_logger.write("SmallestAbsoluteValue");
        m_logger.writeln(props.getProperty("smallestAbsoluteValue"));

    }

    /**
     * after the rec has finished, log summary information
     *
     * @param alias1
     * @param rowCount1
     * @param alias2
     * @param rowCount2
     * @param rowsMatched
     * @throws IOException
     */
    private void logSummary(String alias1, int rowCount1, String alias2,
                            int rowCount2, int rowsMatched) throws IOException {
        if (m_outputType.equals("csv")) {
            try {
                initCsvFile(alias1, alias2, "");
                logSummaryToFile(alias1, rowCount1, alias2, rowCount2,
                        rowsMatched);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
        // always log this anyway
        LOGGER.info("Finished reconciliation: "
                + decimalFormatter.format(rowsMatched) + "/"
                + decimalFormatter.format(rowCount1) + " rows of " + alias1
                + " matched with " + alias2 + " ("
                + decimalFormatter.format(rowCount2) + ")");
    }

    /**
     * log a difference to System.Err. WARNING: Slows performance down lots and
     * lots...!
     *
     * @param key
     * @param alias1
     * @param columnName1
     * @param columnValue1
     * @param alias2
     * @param columnName2
     * @param columnValue2
     */
    private void logDifferenceToSystemErr(String key, String alias1,
                                          String columnName1, Object columnValue1, String alias2,
                                          String columnName2, Object columnValue2) {
        StringBuffer sb = new StringBuffer();
        sb.append("INFO: ");
        sb.append("Key " + key + " | ");
        sb.append(alias1 + "." + columnName1 + " = " + columnValue1);
        sb.append(", ");
        sb.append(alias2 + "." + columnName2 + " = " + columnValue2);

        System.err.println(sb.toString());
    }

    /**
     * initialise the csv file given the data input alias names and the key
     * column names
     *
     * @param alias1
     * @param alias2
     * @param keyColumns
     * @throws java.io.IOException
     */
    private void initCsvFile(String alias1, String alias2, String keyColumns)
            throws IOException {
        if (!m_loggerInit) {
            m_logger = new CSVLogger(FILE_LOCATION, false);
            m_logger.setDelimiter(FILE_DELIMITER);
            m_logger.open();

            // write column headers

            StringTokenizer st = new StringTokenizer(keyColumns, m_delimiter);

            // recurse out all the keys as separate columns to make sorting in
            // excel
            // easier.
            while (st.hasMoreTokens()) {
                m_logger.write("Key(" + st.nextToken() + ")");
            }
            m_logger.write(alias1 + ".columnName");
            m_logger.write(alias1 + ".columnValue");
            m_logger.write(alias2 + ".columnName");
            m_logger.write(alias2 + ".columnValue");
            m_logger.write("% Diff");
            m_logger.writeln("Abs Diff");

            m_loggerInit = true;
        }
    }

}
