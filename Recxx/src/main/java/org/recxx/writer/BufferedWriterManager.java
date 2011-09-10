package org.recxx.writer;

import static org.recxx.utils.CloseableUtils.tryToClose;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class BufferedWriterManager {

	private FileWriter fileWriter;

	private BufferedWriter bufferedWriter;

	public BufferedWriter open(File file) throws IOException {
		fileWriter = new FileWriter(file);
		bufferedWriter = new BufferedWriter(fileWriter);
		return bufferedWriter;
	}

	public void close() throws IOException {
		IOException lastException = tryToClose(bufferedWriter);
		lastException = tryToClose(fileWriter);
		if (lastException != null) {
			throw lastException;
		}
	}
}