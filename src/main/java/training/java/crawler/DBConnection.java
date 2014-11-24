package training.java.crawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * DBConnection gets connection to DB and close it.
 */
public class DBConnection {

	private final String dbDriver;
	private final String dbUrl;
	private final String dbUser;
	private final String dbPass;

	public DBConnection() throws IOException {
		final Properties props = new Properties();
		final InputStream input = new FileInputStream("conf/db.properties");
		props.load(input);

		this.dbDriver = props.getProperty("db.default.driver");
		this.dbUrl = props.getProperty("db.default.url");
		this.dbUser = props.getProperty("db.default.user");
		this.dbPass = props.getProperty("db.default.password");
	}

	public Connection getConnection() throws ClassNotFoundException,
			SQLException {
		Class.forName(dbDriver);
		return DriverManager.getConnection(dbUrl, dbUser, dbPass);
	}

	public void closeConnection(Connection con) {
		if (con != null) {
			try {
				con.close();
				Crawler.LOGGER.info("DB Connection closed successfully");
			} catch (SQLException sqle) {
				Crawler.LOGGER.error("Exception while closing connection" + sqle);
			}
		}
	}

	public void closeStatement(Statement st) {
		if (st != null) {
			try {
				st.close();
				Crawler.LOGGER.info("Statement closed successfully");
			} catch (SQLException sqle) {
				Crawler.LOGGER.error("Exception while closing statement" + sqle);
			}
		}
	}
}