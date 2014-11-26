package training.java.crawler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Download links contains downloadLinks method which downloads mails from Urls
 */
public class Downloader {
	private static final Logger LOGGER = Logger.getLogger(Downloader.class);

	public void downloadUrl(Set<URL> urlSet, BufferedWriter bwriter,
			boolean resume) throws Exception {
		Set<URL> dbUrlSet = new HashSet<URL>();
		
		Pattern pattern = Pattern.compile(".*/raw/.*/.*");

		try (DBOperator dbOperator = new DBOperator()) {
			Connection con = dbOperator.getConnecter();
			Statement st = dbOperator.getStatement(con);

			if (!resume) {
				dbOperator.dbClean(st);
			}

			/*
			 * Traversing the urlSet for urls and check if it's already
			 * downloaded else download and mark it as download by inserting
			 * into database
			 */
			for (URL url : urlSet) {	
				Matcher matcher = pattern.matcher(url.toString());

				if (matcher.matches()) {
					if (!resume || !dbOperator.isDownloaded(st, url)) {
						bwriter.write(getDocument(url.toString()).text()
								.toString() + "\n");
						dbUrlSet.add(url);
					}
					LOGGER.info("Downloading mails from:" + url);
				}
			}
			dbOperator.dbBatchUpdate(st, dbUrlSet);
		}
	}

	public Document getDocument(String string) throws IOException {
		// FIXME get source for this
		return Jsoup.connect(string).ignoreContentType(true).timeout(1 * 1000)
				.ignoreHttpErrors(true).get();
	}
}