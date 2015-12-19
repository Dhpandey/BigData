package hive.BigHive;

import java.sql.SQLException;

public class CreateHqlDb {

	public static void createDb(String dbName) throws SQLException {
		HqlConnection.connection().execute("CREATE DATABASE " + dbName);
		HqlConnection.connection().close();
		System.out.println("Database" + dbName + " created successfully.");

	}
}
