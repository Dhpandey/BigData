package hive.BigHive;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Movies {
	public static void getData() throws SQLException {
		String query = "SELECT Movies.movie_id, AVG(Ratings.rating) avg_rating, Movies.title FROM Movies"
				+ " JOIN Ratings ON (Movies.movie_id = Ratings.movie_id AND Movies.genre LIKE '%Action%' ) "
				+ "GROUP BY Movies.movie_id, Movies.title ORDER BY avg_rating DESC	LIMIT 6";
		// find top 11 average rated "Action" movies with descending order of
		// rating.
		System.out.println(":::Taking time to fetch data :::");
		ResultSet res = HqlConnection.connection().executeQuery(query);
		System.out.println("");

		System.out.println("Result ::::::::::::::::::::");
		System.out.println(" Movie Id \t Title \t Average");
		System.out.println("____________________________________________________________________");

		while (res.next()) {
			System.out.println(res.getInt(1) + "   " + res.getString(3) + "    " + res.getDouble(2));
		}
		HqlConnection.connection().close();
	}

	public static void main(String[] args) throws SQLException {
		// Warning :if new database is created to use it change the
		// Hqlconnection class localhost/dbname
		//CreateHqlDb.createDb("Movies");
		CreateHqlTable.createTables();
		LoadDataToHql.loadData();
		Movies.getData();

	}
}
