package hive.BigHive;

import java.sql.SQLException;

public class LoadDataToHql {
	public  static void loadData() throws SQLException {
		// load data to movies table
		HqlConnection.connection().execute("LOAD DATA LOCAL INPATH '/data/movies_new.dat' OVERWRITE INTO TABLE Movies");
		System.out.println("loaded to Movies successfully");

		// load data to ratings
		HqlConnection.connection()
				.execute("LOAD DATA LOCAL INPATH '/data/ratings_new.dat' OVERWRITE INTO TABLE Ratings");
		System.out.println("loaded to ratings successfully");

		HqlConnection.connection().close();

	}

}
