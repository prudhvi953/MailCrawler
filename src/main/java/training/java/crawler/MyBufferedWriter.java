package training.java.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class MyBufferedWriter implements AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(MyBufferedWriter.class);
	private final boolean append;
	private final File file;

	public MyBufferedWriter(String outFile, boolean append) throws IOException {
		this.append = append;
		this.file = new File(outFile);
		this.createFile();
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
		FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
		return new BufferedWriter(fw);
	}
	
	@Override
	public void close() throws Exception {
		LOGGER.info("BufferedWriter closed");
		
	}
}
