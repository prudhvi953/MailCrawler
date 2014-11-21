package sample.java.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class GetBufferedWriter {
	final static Logger LOGGER = Logger.getLogger(GetBufferedWriter.class);

	private boolean resume = false;
	private File file;

	public GetBufferedWriter(String outFile, boolean resume) {
		this.resume = resume;
		this.file = new File(outFile);
	}

	public void createFile() throws IOException {
		if (!file.exists()) {
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
		}
	}

	public BufferedWriter getWriter() throws IOException {
		FileWriter fw = new FileWriter(file.getAbsoluteFile(), resume);
		BufferedWriter bw = new BufferedWriter(fw);
		return bw;
	}

	public void closeWriter(BufferedWriter bwriter) {
		if (bwriter != null) {
			try {
				bwriter.close();
				LOGGER.info("BufferedWriter closed successfully");
			} catch (IOException ioe) {
				LOGGER.error("Exception while closing BufferedWriter" + ioe);
			}
		}
	}
}