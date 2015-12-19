package lab1.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PairTest {

	MapDriver<LongWritable, Text, Pair, IntWritable> mapDriver;
	ReduceDriver<Pair, IntWritable, Pair, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Pair, IntWritable, Pair, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		PairMapper mapper = new PairMapper();
		PairReducer reducer = new PairReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void mapperTest() {
		mapDriver.withInput(new LongWritable(), new Text("cat,mat,cat,mat"));

		mapDriver.withOutput(new Pair("cat", "mat"), new IntWritable(1));
		mapDriver.withOutput(new Pair("cat", "*"), new IntWritable(1));
		mapDriver.withOutput(new Pair("mat", "cat"), new IntWritable(1));
		mapDriver.withOutput(new Pair("mat", "*"), new IntWritable(1));
		mapDriver.withOutput(new Pair("cat", "mat"), new IntWritable(1));
		mapDriver.withOutput(new Pair("cat", "*"), new IntWritable(1));

		mapDriver.runTest();
	}

	@Test
	public void reducerTest() throws IOException {
		List<IntWritable> count = new ArrayList<IntWritable>();
		count.add(new IntWritable(1));
		count.add(new IntWritable(1));
		List<IntWritable> count1 = new ArrayList<IntWritable>();
		count1.add(new IntWritable(1));
		count1.add(new IntWritable(1));

		reduceDriver.withInput(new Pair("cat", "*"), count1);
		reduceDriver.withInput(new Pair("cat", "mat"), count);
		reduceDriver.withOutput(new Pair("cat", "mat"), new DoubleWritable(2));
		// reduceDriver.runTest();
		reduceDriver.run();

	}

	@Test
	public void mapReduceTest() throws IOException {

		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"cat,mat,cat,mat,pat"));
		mapReduceDriver.addOutput(new Pair("cat", "mat"), new DoubleWritable(0.6666666666666666));
		mapReduceDriver.addOutput(new Pair("cat", "pat"), new DoubleWritable(0.3333333333333333));
		mapReduceDriver.addOutput(new Pair("mat", "cat"), new DoubleWritable(0.5));
		mapReduceDriver.addOutput(new Pair("mat", "pat"), new DoubleWritable(0.5));

		mapReduceDriver.runTest();

	}

}
