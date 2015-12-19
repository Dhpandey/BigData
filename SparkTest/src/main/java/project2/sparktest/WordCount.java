package project2.sparktest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide input aand ouptut correctly !!");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("project2.sparktest")
				.setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile(args[0]);

		JavaPairRDD<String, Integer> counts = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(args[1]);
		context.close();
	}
}
