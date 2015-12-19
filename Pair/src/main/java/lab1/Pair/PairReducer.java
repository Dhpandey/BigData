package lab1.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PairReducer extends MapReduceBase implements
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	double marginal = 0.0;

	public void reduce(Pair key, Iterator<IntWritable> count,
			OutputCollector<Pair, DoubleWritable> output, Reporter arg3)
			throws IOException {

		int sum = 0;
		if (key.getU().toString().equals("*")) {
			marginal = 0.0;
			while (count.hasNext()) {
				IntWritable i = count.next();
				marginal += i.get();
			}
		} else {
			while (count.hasNext()) {
				IntWritable i = count.next();
				sum += i.get();
			}
			double t= sum / marginal;
			System.out.println(key + "," + t);
			output.collect(key, new DoubleWritable(sum / marginal));
		}

	}
}
