package training.java.crawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * DBConnection gets connection to DB and close it.
 */
public class DBConnecter implements AutoCloseable {
	private static final Logger LOGGER = Logger.getLogger(DBConnecter.class);
	
	private final String dbDriver;
	private final String dbUrl;
	private final String dbUser;
	private final String dbPass;

	public DBConnecter()  throws IOException {
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

	public void closeStatement(Statement st) throws SQLException {
		if (st != null) {
			try {
				st.close();
				LOGGER.info("Statement closed successfully");
			} catch (SQLException sqle) {
				LOGGER.error("Exception while closing statement" + sqle);
				throw new IllegalStateException();
			}
		}
	}
	@Override
	public void close() throws Exception {
		LOGGER.info("DBConnection closed");
		
	}
}