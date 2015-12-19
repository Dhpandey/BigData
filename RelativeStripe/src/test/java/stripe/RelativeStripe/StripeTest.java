package stripe.RelativeStripe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.junit.Ignore;
import org.junit.Test;

public class StripeTest {

	MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
	ReduceDriver<Text, MapWritable, Text, Text> reducerDriver;

	MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		StripeMapper mapper = new StripeMapper();
		StripeReducer reducer = new StripeReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reducerDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Ignore
	@Test
	public void mapperTest() {

		MapWritable expOutput1 = new MapWritable();
		expOutput1.put(new Text("bat"), new IntWritable(2));

		HashMap<Text, IntWritable> expOutput2 = new HashMap<Text, IntWritable>();
		expOutput2.put(new Text("bat"), new IntWritable(2));

		mapDriver.withInput(new LongWritable(), new Text("cat,bat,cat"));
		//
		//mapDriver.withOutput(new Text("rat"), expOutput2);
		//mapDriver.withOutput(new Text("cat"), expOutput1);
		mapDriver.runTest();
	}

	@Test
	public void reducerTest() throws IOException {
		List<MapWritable> list = new ArrayList<MapWritable>();
		MapWritable reducerinput1 = new MapWritable();
		Writable key = (Writable) (new Text("cat"));
		Writable value = (Writable) (new IntWritable(2));
		reducerinput1.put(key, value);
		list.add(reducerinput1);

		MapWritable reducerinput2 = new MapWritable();
		Writable key1 = (Writable) (new Text("pat"));
		Writable value1 = (Writable) (new IntWritable(2));
		reducerinput2.put(key1, value1);
		list.add(reducerinput2);

		MapWritable reducerinput3 = new MapWritable();
		Writable key2 = (Writable) (new Text("pat"));
		Writable value2 = (Writable) (new IntWritable(2));
		reducerinput2.put(key2, value2);
		list.add(reducerinput3);

		Text reducerOutput = new Text("[(pat,1)](cat,1)]]");

		reducerDriver.withInput(new Text("rat"), list);
		reducerDriver.withOutput(new Text("rat"), reducerOutput);
		reducerDriver.run();

	}

	@Test
	public void mapReduceTest() throws IOException {

		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"cat,mat,rat,cat"));
		Text reducerOutput1 = new Text("[(rat,0.0)(mat,0.0)]");
		Text reducerOutput2 = new Text("[(cat,0.0)(rat,0.0)]");
		Text reducerOutput3 = new Text("[(cat,1.0)]");

		mapReduceDriver.addOutput(new Text("cat"), reducerOutput1);
		mapReduceDriver.addOutput(new Text("bat"), reducerOutput2);
		mapReduceDriver.addOutput(new Text("mat"), reducerOutput3);

		mapReduceDriver.run();
		// mapReduceDriver.run();

	}
}
