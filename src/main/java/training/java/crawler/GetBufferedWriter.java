package training.java.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class GetBufferedWriter {

	final private boolean resume;
	final private File file;

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
		return new BufferedWriter(fw);
	}

	public void closeWriter(BufferedWriter bwriter) throws IOException {
		if (bwriter != null) {
			try {
				bwriter.close();
				Crawler.LOGGER.info("BufferedWriter closed successfully");
			} catch (IOException ioe) {
				Crawler.LOGGER.error("Exception while closing BufferedWriter" + ioe);
			}
		}
	}
}