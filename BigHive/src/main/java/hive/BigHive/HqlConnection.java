package hive.BigHive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HqlConnection {

	private final static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static Statement connection() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}	
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/moviedb", "hadoop2", "");
		Statement stmt = con.createStatement();
		return stmt;
	}
}
