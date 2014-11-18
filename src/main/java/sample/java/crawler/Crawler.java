package sample.java.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
 
/**
 * Crawler class contains main method which invokes crawlLinks for crawling the links
 * and downloadLinks for downloading the mails from links
 */
public class Crawler {
	static Logger log = Logger.getLogger(Crawler.class); // Log4j logger for logging
	static Set<String> urlSet = new HashSet<String>(); // Set for storing urls
	static String[] urlArray = new String[1000]; //Array for storing urls
	
	/** 
	 * crawlLinks recursively crawls all the urls and store them in 
	 * urlSet if url contains mails in the specific given year
	 * 
	 * @param url
	 *           url which is to be crawled and all the links in that url
	 */	
	public static void crawlLinks(String url) throws IOException {
		int index=0; /* index points to last link that has been fetched*/
		int tail=-1; /* tail points to last link that has been crawled */
		
		while(index!=tail) { 
			if(tail>-1) {
				if(urlArray[tail]=="") {
					log.info("Overflow");
					return;
				}
				url=urlArray[tail];
			} else {
				index=-1;
			}
			Document doc = Jsoup.connect(url).ignoreContentType(true).timeout(1*1000)
							.ignoreHttpErrors(true).get();  
			Elements links = doc.select("a");
		
			for (Element link : links) {
				String absurl = link.attr("abs:href");
				/* Check if url is not crawled already and is of given year */	
				if (absurl.contains("http://mail-archives.apache.org/mod_mbox/maven-users/201411")) { 
						if(!urlSet.contains(absurl)) {			
							urlSet.add(absurl);
							urlArray[++index]=absurl;
							log.info(absurl);
					/* Do the steps for every link in the given url*/		 
					//crawlLinks(absurl);
						}
				}
			}
			tail++;
		}
	}
	
	/**
	 * getWriter returns a new BufferedWriter for writing into
	 * output file.
	 * 
	 * @param outFile
	 *               name of the output file
	 * @param resume
	 * 				 flag whether to resume or not
	 * @return BufferedWriter
	 *                      required for writing mails into outFile
	 */
	public static BufferedWriter getWriter(String outFile,boolean resume) throws IOException {
		/* New FileWriter and BufferedWriter for writing mails */
		File file = new File(outFile);
		FileWriter fw = new FileWriter(file.getAbsoluteFile(),resume);
		BufferedWriter bw = new BufferedWriter(fw);
		
		/* Create file if it doesn't exist as well as parent directories */
		if (!file.exists()) {
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
		}
		
		return bw;
	}
	
	/**
	 * downloadLinks writes mails to the outFile provided as parameter
	 * 
	 * @param outFile
	 *               file in which mails have to be written
	 * @param resume
	 *               whether to resume the previous unfinished process or not
	 */
	public static void downloadLinks(String outFile, boolean resume)
			throws IOException, ClassNotFoundException, SQLException {
		/* For writing mails into file */
		BufferedWriter bwriter = getWriter(outFile, resume);
		
		/* Getting connection for database */
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root", "root");
		Statement st = con.createStatement();
		ResultSet rs = null;
		
        /* Clear the contents of table if resume is false */ 	
        if(!resume) {
        	try {
        		st.executeUpdate("delete from link");
        		log.info("Previous run is cleared");
        	} catch (SQLException e) {
            	log.info("Encountered SQLException:"+e);
        	}
        }
    
        /*
         *  Traversing the urlSet for urls and check if it's already downloaded 
         *  else download and mark it as download by inserting into database
         */        
        for (String iterator : urlSet) {
           if (Pattern.matches(".*/raw/.*/.*", iterator)) {        
        	   try {        	   
        		   rs = st.executeQuery("select * from link where url='"+iterator+"'");
               } catch (SQLException e) {
            	   log.info("Encountered SQLException:"+e);
               }
        	   
        	   if (!rs.isBeforeFirst()) { 
        		   bwriter.write(Jsoup.connect(iterator).ignoreContentType(true).timeout(1*1000)
						    .ignoreHttpErrors(true).get().text().toString()
						    + "\n\n\n\n");
        	   /* Marking this url as downloaded for future reference */
        		   try {
        			   st.executeUpdate("insert into link(url,isDownloaded) values ('"+iterator+"',1)");
        		   } catch (SQLException e) {
        			   log.info("Encountered SQLException:"+e);
        		   }
               } else {
            	   log.info("Skipped as already downloaded");
			}
        	   log.info("Downloading mails from:" + iterator);   
           }
        }
        
        /* Closing statement, connection and BufferedWriter */
		if(st !=null) {
			try {
				st.close();
			} catch(SQLException e) {
				log.info("Exception while closing statement:"+e);
			}
		}
		
		if(con !=null) {
			try {
				con.close();
			}
			catch(SQLException e){
				log.info("Exception while closing connection:"+e);
			}
		}
		
		if(bwriter !=null) {
			try {
				bwriter.close();
			}
			catch(IOException e){
				log.info("Exception while closing BufferedWriter:"+e);
			}
		}
}

	public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException {
		String startURL = "http://mail-archives.apache.org/mod_mbox/maven-users/";
		String outFile = "output";
        boolean resume = false;
        
		crawlLinks(startURL); /* CrawlingLinks from start URL */
		downloadLinks(outFile, resume); /* Writing mails to output file */
	}
}
