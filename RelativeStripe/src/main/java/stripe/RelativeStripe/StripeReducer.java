package stripe.RelativeStripe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StripeReducer extends MapReduceBase implements
		Reducer<Text, MapWritable, Text, Text> {

	int marginal = 0;

	@Override
	public void reduce(Text item, Iterator<MapWritable> values,
			OutputCollector<Text, Text> output, Reporter re) throws IOException {
		// MapWritable mapW = new MapWritable();
		Map<String, Double> map = new HashMap<String, Double>();

		// MapWritable result = new MapWritable();
		Map<String, Double> result = new HashMap<String, Double>();
		StringBuilder finalResult = new StringBuilder();

		while (values.hasNext()) {
			marginal = 0;
			MapWritable p = values.next();
			p.forEach((k, v) -> {
				if (!map.containsKey(k)) {
					map.put(k.toString(), new Double((((IntWritable) v).get())));
					marginal += ((IntWritable) v).get();
				} else {
					double va = map.get(k)
							+ new Double((((IntWritable) v).get()));
					map.put(k.toString(), va);
					marginal += ((IntWritable) v).get();
				}
			});
		}

		map.forEach((k, v) -> {
			double frequency = v / marginal;
			result.put(k.toString(), frequency);
		});

		finalResult.append("[");
		result.forEach((k, v) -> {
			finalResult.append("(" + k + "," + v + ")");
		});
		finalResult.append("]");
		output.collect(item, new Text(finalResult.toString()));

	}

}
