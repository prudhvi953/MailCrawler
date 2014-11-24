package training.java.crawler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Crawler crawls and downloads all the mails for the year 2014
 */
public class Crawler {
	public static final Logger LOGGER = Logger.getLogger(Crawler.class);

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, SQLException {
		final String START_URL = "http://mail-archives.apache.org/mod_mbox/maven-users/";
		final int ARGS_NUM = 2;
		
		Set<String> urlSet = new HashSet<String>();
		String outFile;
		boolean resume;

		if (args.length == ARGS_NUM) {
			outFile = args[0];
			resume = Boolean.valueOf(args[1]);
		} else {
			throw (new IllegalArgumentException(
					"Two arguments must be provided"));
		}

		CrawlLinks crawler = new CrawlLinks();
		urlSet = crawler.crawlUrl(START_URL);

		DownloadLinks download = new DownloadLinks(urlSet);
		download.downloadLinks(outFile, resume);
	}
}