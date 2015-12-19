package hive.BigHive;

import java.sql.SQLException;

public class CreateHqlTable {

	public static void createTables() throws SQLException {

		String dropMovies = "DROP TABLE IF EXISTS Movies";
		HqlConnection.connection().execute(dropMovies);

		String query1 = "CREATE TABLE Movies (movie_id INT, title STRING, genre STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\043' LINES TERMINATED BY '\n'";
		HqlConnection.connection().execute(query1);
		System.out.println("Table Movies Created !!");

		String dropRatings = "DROP TABLE IF EXISTS Ratings";
		HqlConnection.connection().execute(dropRatings);

		String query2 = "CREATE TABLE Ratings (user_id INT, movie_id INT, rating INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\043' LINES TERMINATED BY '\n'";
		HqlConnection.connection().execute(query2);
		System.out.println("Table Ratings Created !!");
		HqlConnection.connection().close();
	}
}
