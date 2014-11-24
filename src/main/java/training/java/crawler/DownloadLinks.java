package training.java.crawler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Download links contains downloadLinks method which
 * downloads mails from Urls
 */ 
public class DownloadLinks {

	private static final int MAX_TIMEOUT = 1 * 1000;
	private static final boolean IGNORE_HTTP_ERRORS = true;
	private static final boolean IGNORE_CONTENT_TYPE = true;

	private Set<String> urlSet = new HashSet<String>();

	public DownloadLinks(Set<String> urlSet) {
		this.urlSet = urlSet;
	}

	public void downloadLinks(String outFile, boolean resume)
			throws IOException, ClassNotFoundException, SQLException {

		GetBufferedWriter bufWriter = new GetBufferedWriter(outFile, resume);
		bufWriter.createFile();
		BufferedWriter bwriter = bufWriter.getWriter();

		Pattern pattern = Pattern.compile(".*/raw/.*/.*");

		DBConnection dbCon = new DBConnection();
		Connection con = dbCon.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = null;

		if (!resume) {
			try {
				st.executeUpdate("delete from link");
				Crawler.LOGGER.info("Previous run is cleared");
			} catch (SQLException sqle) {
				Crawler.LOGGER.error("Encountered SQLException:" + sqle);
			}
		}

		/*
		 * Traversing the urlSet for urls and check if it's already downloaded
		 * else download and mark it as download by inserting into database
		 */
		for (String url : urlSet) {
			Matcher matcher = pattern.matcher(url);

			if (matcher.matches()) {
				try {
					rs = st.executeQuery("select * from link where url='" + url
							+ "'");
				} catch (SQLException e) {
					Crawler.LOGGER.error("Encountered SQLException:" + e);
				}

				if (!rs.isBeforeFirst()) {
					bwriter.write(getDocument(url).text().toString() + "\n");
					try {
						st.executeUpdate("insert into link(url,isDownloaded) values ('"
								+ url + "',1)");
					} catch (SQLException e) {
						Crawler.LOGGER.error("Encountered SQLException:" + e);
					}
				} else {
					Crawler.LOGGER.info("Skipped as already downloaded");
				}
				Crawler.LOGGER.info("Downloading mails from:" + url);
			}
		}

		dbCon.closeConnection(con);
		dbCon.closeStatement(st);
		bufWriter.closeWriter(bwriter);
	}

	public Document getDocument(String url) throws IOException {
		return Jsoup.connect(url).ignoreContentType(IGNORE_CONTENT_TYPE)
				.timeout(MAX_TIMEOUT).ignoreHttpErrors(IGNORE_HTTP_ERRORS)
				.get();
	}
}