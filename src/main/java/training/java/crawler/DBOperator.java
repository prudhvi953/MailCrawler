package training.java.crawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * DBConnection gets connection to DB and close it.
 */
public class DBOperator implements AutoCloseable {
	private static final Logger LOGGER = Logger.getLogger(DBOperator.class);
	
	private final String dbDriver;
	private final String dbUrl;
	private final String dbUser;
	private final String dbPass;

	public DBOperator()  throws IOException {
		final Properties props = new Properties();
		final InputStream input = new FileInputStream("conf/db.properties");
		props.load(input);
		
		this.dbDriver = props.getProperty("db.default.driver");
		this.dbUrl = props.getProperty("db.default.url");
		this.dbUser = props.getProperty("db.default.user");
		this.dbPass = props.getProperty("db.default.password");
	}

	public Connection getConnecter() throws ClassNotFoundException,
			SQLException {
		Class.forName(dbDriver);
		return DriverManager.getConnection(dbUrl, dbUser, dbPass);
	}
	
	public Statement getStatement(Connection con) throws SQLException {
		return con.createStatement();
	}

	public void dbClean(Statement st) {
		dbUpdate(st, "delete from link");
	}

	public void dbUpdate(Statement st, String query) {
		try {
			st.executeUpdate(query);
		} catch (SQLException sqle) {
			LOGGER.error("Encountered SQLException:" + sqle);
			throw new IllegalStateException();
		}		
	}
	
	public void dbBatchUpdate(Statement st, Set<URL> dbUrlSet) {
		for (URL dbUrl : dbUrlSet) {
			dbUpdate(st, "insert into link(url,isDownloaded) values ('"
					+ dbUrl.toString() + "',1)");
		}
	}

	public boolean isDownloaded(Statement st, URL url) throws SQLException {
		try(ResultSet rs = st.executeQuery("select * from link where url='"
							+ url.toString() + "'")) {
			if(!rs.next()) {
			return false;	
			}
		}
		return true;
	}
	
	@Override
	public void close() throws Exception {
		LOGGER.info("DBConnection closed");		
	}
}
