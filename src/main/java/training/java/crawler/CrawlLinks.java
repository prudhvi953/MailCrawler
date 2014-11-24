package training.java.crawler;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * CrawlLinks contains crawlLinks method which downloads mails from Urls
 */
public class CrawlLinks {
	private static final int MAX_TIMEOUT = 1 * 1000;
	private static final boolean IGNORE_HTTP_ERRORS = true;
	private static final boolean IGNORE_CONTENT_TYPE = true;

	private static final String VALID_URL = "http://mail-archives.apache.org/mod_mbox/maven-users/201411";
	private static final String ANCHOR_TAG = "a";
	private static final String ABS_HREF = "abs:href";

	public Set<String> crawlUrl(String url) throws IOException {
		Set<String> urlSet = new HashSet<String>();
		Queue<String> urlQueue = new LinkedList<String>();

		urlQueue.add(url);
		Iterator<String> iterator = urlQueue.iterator();

		while (iterator.hasNext()) {
			url = urlQueue.remove();
			Document doc = getDocument(url);
			Elements links = doc.select(ANCHOR_TAG);

			for (Element link : links) {
				String absUrl = link.attr(ABS_HREF);
				if (absUrl.contains(VALID_URL) && !urlSet.contains(absUrl)) {
					urlSet.add(absUrl);
					urlQueue.add(absUrl);
					Crawler.LOGGER.info("Crawled:" + absUrl);
				}
			}
		}
		return urlSet;
	}

	public Document getDocument(String url) throws IOException {
		return Jsoup.connect(url).ignoreContentType(IGNORE_CONTENT_TYPE)
				.timeout(MAX_TIMEOUT).ignoreHttpErrors(IGNORE_HTTP_ERRORS)
				.get();
	}
}