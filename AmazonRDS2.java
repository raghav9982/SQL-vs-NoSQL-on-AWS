
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import net.spy.memcached.MemcachedClient;

public class AmazonRDS2 {

	public static void main(String[] args) {

		ArrayList<String> arLimitedScope = new ArrayList<>();
		ArrayList<String> arRandom = new ArrayList<>();

		// Read RDS Connection Information from the Environment
		String dbName = "kkkk";// System.getProperty("RDS_DB_NAME");
		String userName = "kkkk";// System.getProperty("RDS_USERNAME");
		String password = "kkkkkkkk"; // System.getProperty("RDS_PASSWORD");
		String hostname = "kkkk.crqgr1intbbb.us-west-2.rds.amazonaws.com"; // System.getProperty("RDS_HOSTNAME");
		String port = "3306"; // System.getProperty("RDS_PORT");
		String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName
				+ "?user=" + userName + "&password=" + password;

		// Load the JDBC Driver
		try {
			System.out.println("Loading driver...");
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Driver loaded!");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
					"Cannot find the driver in the classpath!", e);
		}

		Connection conn = null;
		Statement setupStatement = null;
		Statement readStatement = null;
		ResultSet resultSet = null;
		String results = "";
		int numresults = 0;
		String statement = null;

		try {
			// Create connection to RDS instance
			conn = DriverManager.getConnection(jdbcUrl);

			// Execute 2000 queries

			 //arLimitedScope = new GenerateQueries().limitedScopeQueries();
			arRandom = new GenerateQueries().Queries();

			System.out.println("Queries Generated.. \n");
			//new AmazonRDS2().execute2000Queries(conn, arLimitedScope);
			//new AmazonRDS2().execute2000Queries(conn, arRandom);

			new AmazonRDS2().execute2000Queries(conn, arRandom);
			/*
			 * // Create a table and write two rows setupStatement =
			 * conn.createStatement(); String createTable =
			 * "CREATE TABLE Beanstalk1 (Resource char(50));"; String insertRow1
			 * = "INSERT INTO Beanstalk1 (Resource) VALUES ('EC21 Instance');";
			 * String insertRow2 =
			 * "INSERT INTO Beanstalk1 (Resource) VALUES ('RDS1 Instance');";
			 * 
			 * setupStatement.executeUpdate(createTable);
			 * setupStatement.executeUpdate(insertRow1);
			 * setupStatement.executeUpdate(insertRow2);
			 */

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
			System.out.println("Closing the connection.");
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException ignore) {
				}
		}
	}

	public void execute2000Queries(Connection conn, ArrayList<String> ar) {
		try {
			
			Statement setupStatement = conn.createStatement();
			System.out.println("Executing the queries ....");
			long startTime = System.currentTimeMillis();
			
			for (int i = 0; i < ar.size(); i++) {
					setupStatement.executeQuery(ar.get(i));
				}
			long endTime = System.currentTimeMillis();

			System.out.println("Time Taken for 500 queries :"
					+ (endTime - startTime) + " milliseconds or "
					+ (endTime - startTime) / 1000 + " seconds.");
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}

	public void findMagnitude(Connection conn) {

		String difference = "DATEDIFF(curdate(),str_to_date(substring(time1,1,10), '%Y-%m-%d'))";
		String query;
		ResultSet rs;
		int result;
		try {

			Statement stmt = conn.createStatement();
			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " <30 and mag >=2 and mag<3;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = rs.getInt("count");
			System.out.println("Magnitude >2 in last 30 days :"
					+ rs.getInt("count"));

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " >30 and mag >=2 and mag<3;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = result - rs.getInt("count");
			System.out.println("Magnitude >2 before 30 days :" + result);

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " <30 and mag >=3 and mag<4;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = rs.getInt("count");
			System.out.println("\nMagnitude >3 in last 30 days :"
					+ rs.getInt("count"));

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " >30 and mag >=3 and mag<4;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = result - rs.getInt("count");
			System.out.println("Magnitude >3 before 30 days :" + result);

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " <30 and mag >=4 and mag<5;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = rs.getInt("count");
			System.out.println("\nMagnitude >4 in last 30 days :"
					+ rs.getInt("count"));

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " >30 and mag >=4 and mag<5;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = result - rs.getInt("count");
			System.out.println("Magnitude >4 before 30 days :" + result);

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " <30 and mag >=5;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = rs.getInt("count");
			System.out.println("\nMagnitude >=5 in last 30 days :"
					+ rs.getInt("count"));

			query = "select count(mag) as count  from earthquakes2 where "
					+ difference + " >30 and mag >=5;";
			rs = stmt.executeQuery(query);
			rs.next();
			result = result - rs.getInt("count");
			System.out.println("Magnitude >=5 before 30 days :" + result);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
