
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.opencsv.CSVReader;

public class AmazonRDS3 {

	public static void main(String[] args) {

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

			//new AmazonRDS3().readCsv(conn);
			//new AmazonRDS3().readfromCSV(conn);
			 
			// Create a table
			setupStatement = conn.createStatement();
			
			
			
			String query1 ="LOAD DATA LOCAL INFILE 'venkat.csv' INTO TABLE venkat FIELDS TERMINATED BY ','" + " LINES TERMINATED BY '\n'"; // (txn_amount, card_number, terminal_id) ";
			
			System.out.println("Uploading the data into RDS...");
			//String query = "LOAD DATA INFILE 'D:\\Reading Zone\\MS\\Third Semester\\Cloud\\all_month.csv' INTO TABLE testtable FIELDS TERMINATED BY ','";
			long startTime = System.currentTimeMillis();
			setupStatement.execute(query1);
			long endTime = System.currentTimeMillis();
			
			System.out.println("\nTotal time taken for uploading .."+(endTime - startTime)+ " milli seconds or "+(endTime - startTime)/1000+" seconds\n");
			 
			/*String createTable ="CREATE TABLE Beanstalk1 (Resource char(50));"; 
			 String insertRow1 = "INSERT INTO Beanstalk1 (Resource) VALUES ('EC21 Instance');";
			 String insertRow2 = "INSERT INTO Beanstalk1 (Resource) VALUES ('RDS1 Instance');";
			 setupStatement.executeUpdate(createTable);
			 setupStatement.executeUpdate(insertRow1);
			 setupStatement.executeUpdate(insertRow2);*/	 

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
	public void readfromCSV(Connection conn) {
		
		
		try{
			Statement stmt = conn.createStatement();
			BufferedReader br=new BufferedReader(new FileReader("all_month.csv")); 
			String line = br.readLine();
			String[] array=line.split(",");
			System.out.println(array[0]+" : "+array[array.length-1]);
			
			/*while(br.readLine()!=null) {
				
				
			}*/
		}catch(Exception e) {
			
		}
	}
	
	private static void readCsv(Connection connection)
	{

			try (CSVReader reader = new CSVReader(new FileReader("upload.csv"), ',');)
			{
				    
					String insertQuery = "Insert into txn_tbl (txn_id,txn_amount, card_number, terminal_id) values (null,?,?,?)";
					PreparedStatement pstmt = connection.prepareStatement(insertQuery);
					String[] rowData = null;
					int i = 0;
					while((rowData = reader.readNext()) != null){
					for (String data : rowData)
					{
							pstmt.setString((i % 3) + 1, data);

							if (++i % 3 == 0)
									pstmt.addBatch();// add batch

							if (i % 30 == 0)// insert when the batch size is 10
									pstmt.executeBatch();
					}}
					System.out.println("Data Successfully Uploaded");
			}
			catch (Exception e)
			{
					e.printStackTrace();
			}

	}
	
	
}
