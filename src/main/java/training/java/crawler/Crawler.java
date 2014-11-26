package training.java.crawler;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Crawler crawls and downloads all the mails for the year 2014
 */
public class Crawler {
	
	private static final Logger LOGGER = Logger.getLogger(Crawler.class);
	private final int MAX_TIMEOUT;
	private final boolean IGNORE_CONTENT_TYPE;
	private final boolean IGNORE_HTTP_ERRORS;
	private String VALID_URL;
	private String ANCHOR_TAG;
	private String ABS_HREF;

	
	public Crawler() throws IOException {
		final Properties props = new Properties();
		final InputStream input = new FileInputStream("conf/jsoup.properties");
		props.load(input);
		
		this.MAX_TIMEOUT = Integer.parseInt(props.getProperty("jsoup.default.max_timeout"));
		this.IGNORE_CONTENT_TYPE = Boolean.valueOf(props.getProperty("jsoup.default.ignore_content_type"));
		this.IGNORE_HTTP_ERRORS = Boolean.valueOf(props.getProperty("jsoup.default.ignore_http_erros"));
		
		this.VALID_URL = props.getProperty("jsoup.default.valid_url");
		this.ANCHOR_TAG = props.getProperty("jsoup.default.anchor_tag");
		this.ABS_HREF = props.getProperty("jsoup.default.abs_href");
	}
	
	public Set<URL> crawl(URL url) throws IOException {
		Set<URL> urlSet = new HashSet<URL>();
		Queue<URL> urlQueue = new LinkedList<URL>();
		
		do {
			Document doc = getDocument(url.toString());
			Elements links = doc.select(ANCHOR_TAG);
			
			for (Element link : links) {
				String absUrl = link.attr(ABS_HREF);
				if (absUrl.contains(VALID_URL) && !urlSet.contains(new URL(absUrl))) {
					urlSet.add(new URL(absUrl));
					urlQueue.add(new URL(absUrl));
					Crawler.LOGGER.info("Crawled:" + absUrl);
				}
			}
		} while(!urlQueue.isEmpty() && !(url = urlQueue.remove()).equals(null));
		
		//FIXME check if this code can be written without add and subsequent remove of the first element
		/*urlQueue.add(url);
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
		}*/
		
		/*
		 for(String url : urlSet) {
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
		
		} */
		
		return urlSet;
	}

	public Document getDocument(String string) throws IOException {
		//FIXME get source for this
		return Jsoup.connect(string).ignoreContentType(IGNORE_CONTENT_TYPE)
				.timeout(MAX_TIMEOUT).ignoreHttpErrors(IGNORE_HTTP_ERRORS)
				.get();
	}
	
	public static void main(String args[]) throws Exception {
		final int ARGS_NUM = 3;
		
		if (args.length == ARGS_NUM) {
			URL startUrl = new URL(args[0]);
			String outFile = args[1];
			boolean resume = Boolean.valueOf(args[2]);
			
			Crawler crawler = new Crawler();
			Set<URL> urlSet = crawler.crawl(startUrl);
			
			Downloader downloader = new Downloader();
			try (MyBufferedWriter bufWriter = new MyBufferedWriter(outFile, resume)) {
				BufferedWriter bwriter = bufWriter.getWriter();
			downloader.downloadUrl(urlSet, bwriter, resume);
			}
		} else {
			throw new IllegalArgumentException(
					"Two arguments must be provided");
		}
		
	}
}