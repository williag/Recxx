package org.recxx;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.StringTokenizer;

/**
 * Generic class to enable logging of values to a csv file
 * <P>
 * <P>
 * Sample Usage: 1) Create a new instance of CSVLogger (filename must contain an
 * extension like .csv or .txt)
 *
 * <P>
 * CSVLogger foo = new CSVLogger("/home/foo/file.csv", true)
 *
 * <P>
 * 2) Set any parameters needed and prepare the file for logging
 *
 * <P>
 * foo.setFormatStamp(new SimpleDatFormat("ddMMyyyyHHmm"));
 * <P>
 * foo.open();
 *
 * <P>
 * This will create a file /home/foo/file050720001043.csv
 *
 * <P>
 * 3) You can then write out either strings or arrays
 *
 * <P>
 * foo.write("sample");
 * <P>
 * will write out "sample," (the delimiter of a comma is the default)
 *
 * <P>
 * foo.writeln("sample");
 * <P>
 * will write out "sample" followed by a line feed character for a new line
 *
 * <P>
 * foo.writeln(new String[{"I"},{"want"},{"this"},{"text"},{"delimited"}]);
 * <P>
 * will write out "I,want,this,text,delimited" line feed
 *
 * <P>
 * 4) Once finished, close the CSVLogger off
 *
 * <P>
 * foo.close();
 *
 *
 */
public class CSVLogger {
	// internal class variables
	private File _csvFile;
	private FileWriter _fw;
	private BufferedWriter _bw;
	private int _offset = 0;

	private String DELIMITER = ",";
	private String NULL_STRING = "";

	public boolean TIMESTAMP_FILE = false;
	private String FILENAME = "";

	private SimpleDateFormat _formattedStamp = new SimpleDateFormat(
			"ddMMyyyyHHmmss");
	private SimpleDateFormat _formattedReadable = new SimpleDateFormat(
			"HH:mm:ss dd/MM/yyyy");

	static LoggerWrapper m_logger = new LoggerWrapper(CSVLogger.class.getName());

	/**
	 * Filename must contain a valid extension of .csv or .txt
	 *
	 * @param csvFile
	 *            file
	 */
	public CSVLogger(File csvFile) {
		FILENAME = csvFile.getPath();
		_csvFile = csvFile;
	}

	/**
	 * Filename must contain a valid extension of .csv or .txt
	 *
	 * @param filename
	 *            name of file to log to
	 * @param timeStampFile
	 *            whether or not to make the filename containa timestamp
	 */
	public CSVLogger(String filename, boolean timeStampFile) {
		FILENAME = filename;
		TIMESTAMP_FILE = timeStampFile;
	}

	/**
	 * gets the current timestamp and adds it to the filename
	 *
	 */
	private void applyTimeStamp() {
		if ((FILENAME.endsWith(".csv")) || (FILENAME.endsWith(".txt"))) {
			// file is in the correct format, of eg nic.csv
			String s = "";
			StringTokenizer st = new StringTokenizer(FILENAME, ".");

			s = st.nextToken() + getCurrentTimeStamp() + "." + st.nextToken();

			m_logger.info("File " + FILENAME + " changed to " + s);

			FILENAME = s;

		}

		else {
			// file is the wrong format
			// should throw an exception here, but haven't created a custom
			// one...

			m_logger.error("File ["
					+ FILENAME
					+ "] is an invalid format and must contain an extension. HINT: must be in the format foo.csv or foo.txt");
			System.exit(-1);
		}

	}

	/**
	 * closes the CSVLogger cleanly
	 *
	 * @exception java.io.IOException
	 *                problem writing to the file
	 */
	public void close() throws IOException {
		_bw.close();
	}

	/**
	 * override the default finalise method to clean up
	 *
	 */
	public void finalize() {
		// free up resources
		_bw = null;
		_fw = null;
		_csvFile = null;
	}

	/**
	 * gets the current timestamp
	 *
	 * @return the current timestamp in a stringified format
	 */
	public String getCurrentTimeStamp() {
		// 1. Initialise as per GMT:
		SimpleTimeZone stz = new SimpleTimeZone(0, "UK-GMT/BST");

		// 2. Clocks go back an hour on last Sunday in March at 2:00 AM:
		stz.setStartRule(Calendar.MARCH, -1, Calendar.SUNDAY,
				2 * 60 * 60 * 1000);

		// 3. Clocks go forward (to GMT) on last Sunday in October at 2:00 AM:
		stz.setEndRule(Calendar.OCTOBER, -1, Calendar.SUNDAY,
				2 * 60 * 60 * 1000);

		SimpleTimeZone.setDefault(stz);
		_formattedStamp.setTimeZone(stz);

		return _formattedStamp.format((Calendar.getInstance()).getTime());
	}

	/**
	 * get the current delimiter, default is comma (,)
	 *
	 * @return java.lang.String
	 */
	public String getDelimiter() {
		return DELIMITER;
	}

	/**
	 * returns the current format of the inline timestamp
	 *
	 * @return a SimpleDateFormat
	 */
	public SimpleDateFormat getFormatStamp() {
		return _formattedStamp;
	}

	/**
	 * return the default value to be written to file when a null or space is
	 * encountered in the string to be written.
	 *
	 * Note: BCP formatted csv files need ,, to input a null in BCP, while
	 * StringTokenizers reading a formatted CSV file will need , ,
	 *
	 * @return java.lang.String
	 */
	public String getNullString() {
		return NULL_STRING;
	}

	/**
	 * prepares the CSV file for logging, using certain customizable parameters
	 *
	 */
	public void open() {
		if (TIMESTAMP_FILE) {
			applyTimeStamp();
			_csvFile = new File(FILENAME);
		}

		else {
			_csvFile = new File(FILENAME);
		}

		try {
			_fw = new FileWriter(_csvFile);
			_bw = new BufferedWriter(_fw);

			m_logger.info("Created CSV File: " + _csvFile.toString());
		}

		catch (FileNotFoundException fnfe) {
			m_logger.error("FileNotFoundException in init(). Message is "
					+ fnfe.getMessage());
			fnfe.printStackTrace();
			System.exit(-1);
		}

		catch (IOException ioe) {
			m_logger.error("IOException in init(). Message is "
					+ ioe.getMessage());
			ioe.printStackTrace();
			System.exit(-1);
		}

	}

	/**
	 * by default, the seperator is a comma(,) but this can be changed
	 *
	 * @param delimiter
	 *            java.lang.String
	 */
	public void setDelimiter(String delimiter) {
		DELIMITER = delimiter;
	}

	/**
	 * sets the format of the date to appear in the name of the file if
	 * TIMESTAMP_FILE is set to true The Default is ddMMyyyyHHmmss .
	 *
	 * @param SimpleDateFormat
	 *            format for logfile inline timestamp
	 */
	public void setFormatStamp(SimpleDateFormat newFormat) {
		_formattedStamp = newFormat;
	}

	/**
	 * return the default value to be written to file when a null or space is
	 * encountered in the string to be written.
	 *
	 * Note: BCP formatted csv files need ,, to input a null in BCP, while
	 * StringTokenizers reading a formatted CSV file will need , ,
	 *
	 * @param java
	 *            .lang.String
	 */
	public void setNullString(String nullString) {
		NULL_STRING = nullString;
	}

	/**
	 * returns info on the file, its last modification and size
	 *
	 * @return information
	 */
	public String toString() {
		return "CSV File [" + _csvFile.getPath() + "], last modfied at ["
				+ _formattedReadable.format(new Date(_csvFile.lastModified()))
				+ "], size [" + _csvFile.length() + " bytes].";
	}

	/**
	 * Write out a double to file.
	 *
	 * @param value
	 *            double
	 */
	public void write(double value) throws IOException {

		write(String.valueOf(value));
	}

	/**
	 * Write out a float to file
	 *
	 * @param value
	 *            float
	 */
	public void write(float value) throws IOException {

		write(String.valueOf(value));
	}

	/**
	 * Write out an int to file.
	 *
	 * @param value
	 *            int
	 */
	public void write(int value) throws IOException {

		write(String.valueOf(value));
	}

	/**
	 * writes out a single line, appended with a comma, to the file
	 *
	 * @param text
	 *            to write to file
	 * @exception java.io.IOException
	 *                problem writing to the file
	 */
	public void write(String s) throws IOException {
		if (s == null || s.equals(""))
			s = NULL_STRING;

		s += DELIMITER;

		int len = s.length();
		_bw.write(s, 0, len);

		_offset += len;
	}

	/**
	 * Writes out a java.util.Date value to the file, converting it to an ODBC
	 * compliant format.
	 *
	 * @param value
	 *            java.util.Date
	 */
	public void write(Date value) throws IOException {

		if (value != null)
			write(value);
		else
			write(new String());
	}

	/**
	 * Loops through an array and ouputs accordingly, using the delimiter to
	 * separate ever entry, except the last one which is followed by a line
	 * feed.
	 *
	 * @param array
	 *            to write to file
	 * @exception java.io.IOException
	 *                problem writing to the file
	 */
	public void writeln(String[] s) throws IOException {

		for (int i = 0; i < s.length; i++) {
			// look out for nulls
			if (s[i] == null)
				s[i] = NULL_STRING;

			if (i != (s.length - 1))
				// normal entry, so append a DELIMITER
				write(s[i].trim());
			else
				// its the last entry so just append a RETURN
				writeln(s[i].trim());
		}

	}

	/**
	 * writes out a single line followed by a carriage return
	 *
	 * @param text
	 *            to write to file
	 * @exception java.io.IOException
	 *                problem writing to the file
	 */
	public void writeln(String s) throws IOException {
		if (s == null || s.equals(""))
			s = NULL_STRING;

		int len = s.length();
		_bw.write(s, 0, len);
		_bw.newLine();

		_offset += len;
	}

	/**
	 * Write out a java.util.Date value to file, followed by a new line.
	 *
	 * @param value
	 *            java.util.Date
	 */
	public void writeln(Date value) throws IOException {

		if (value != null)
			writeln(value);
		else
			writeln(new String());
	}

	/**
	 * Write out an int to file with a line return
	 *
	 * @param value
	 *            int
	 */
	public void writeln(int value) throws IOException {

		writeln(String.valueOf(value));
	}

	/**
	 * Write out a float to file with a line return
	 *
	 * @param value
	 *            int
	 */
	public void writeln(float value) throws IOException {

		writeln(String.valueOf(value));
	}

	/**
	 * Write out a double to file with a line return
	 *
	 * @param value
	 *            int
	 */
	public void writeln(double value) throws IOException {

		writeln(String.valueOf(value));
	}

}
