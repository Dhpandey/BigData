package db.nosql;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import com.mongodb.ServerAddress;
import java.util.Arrays;

public class MongoDBJDBC {

	public static void main(String args[]) {

		try {

			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			DB db = mongoClient.getDB("profile");
			System.out.println("Connect to database successfully");
			// boolean auth = db.authenticate("", "");
			// System.out.println("Authentication: "+auth);

			DBCollection col = db.getCollection("person");

			// insert
			BasicDBObject doc = new BasicDBObject("name", "Dheeraj Pandey")
					.append("age", "25").append("fav", "Movie");
			col.insert(doc);
			System.out.println("Done !!");

			// retrieve all
			DBCursor cursor = col.find();
			int i = 1;
			while (cursor.hasNext()) {
				System.out.println("Document number ::" + i);
				System.out.println(cursor.next());
				i++;
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
}
