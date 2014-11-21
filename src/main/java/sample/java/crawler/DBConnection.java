package sample.java.crawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBConnection {
	final static Logger LOGGER = Logger.getLogger(DBConnection.class);

	private String dbDriver = "";
	private String dbUrl = "";
	private String dbUser = "";
	private String dbPass = "";

	public DBConnection() throws IOException {
		Properties props = new Properties();
		InputStream input = new FileInputStream("conf/db.properties");
		props.load(input);

		this.dbDriver = props.getProperty("db.default.driver");
		this.dbUrl = props.getProperty("db.default.url");
		this.dbUser = props.getProperty("db.default.user");
		this.dbPass = props.getProperty("db.default.password");
	}

	public Connection getConnection() throws ClassNotFoundException,
			SQLException {
		Class.forName(dbDriver);
		Connection con = DriverManager.getConnection(dbUrl, dbUser, dbPass);
		return con;
	}

	public void closeConnection(Connection con) {
		if (con != null) {
			try {
				con.close();
				LOGGER.info("DB Connection closed successfully");
			} catch (SQLException sqle) {
				LOGGER.error("Exception while closing connection" + sqle);
			}
		}
	}

	public void closeStatement(Statement st) {
		if (st != null) {
			try {
				st.close();
				LOGGER.info("Statement closed successfully");
			} catch (SQLException sqle) {
				LOGGER.error("Exception while closing statement" + sqle);
			}
		}
	}
}