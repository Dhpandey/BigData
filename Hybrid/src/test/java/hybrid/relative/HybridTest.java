package hybrid.relative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class HybridTest {

	MapDriver<LongWritable, Text, Pair, IntWritable> mapDriver;
	ReduceDriver<Pair, IntWritable, Text, Text> reduceDriver;

	MapReduceDriver<LongWritable, Text, Pair, IntWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		HybridMapper mapper = new HybridMapper();
		HybridReducer reducer = new HybridReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void mapperTest() {
		mapDriver.withInput(new LongWritable(), new Text("cat,mat,cat,mat"));
		mapDriver.withOutput(new Pair("cat", "mat"), new IntWritable(1));
		mapDriver.withOutput(new Pair("mat", "cat"), new IntWritable(1));
		mapDriver.withOutput(new Pair("cat", "mat"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void reducerTest() throws IOException {
		List<IntWritable> count = new ArrayList<IntWritable>();
		count.add(new IntWritable(1));
		count.add(new IntWritable(1));

		reduceDriver.withInput(new Pair("cat", "mat"), count);
		reduceDriver.withOutput(new Text("cat"), new Text("[(mat,1)]"));
		// reduceDriver.runTest();
		reduceDriver.run();

	}

	@Test
	public void mapReduceTest() throws IOException {

		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"cat,mat"));
		Text reducerOutput1 = new Text("[(mat,0.5)(rat,0.5)]");
		//Text reducerOutput3 = new Text("[(cat,1.0)]");

		mapReduceDriver.addOutput(new Text("cat"), reducerOutput1);
		//mapReduceDriver.addOutput(new Text("mat"), reducerOutput3);

		mapReduceDriver.runTest(); // mapReduceDriver.run();

	}

}
