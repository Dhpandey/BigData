package hybrid.relative;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HybridReducer extends MapReduceBase implements
		Reducer<Pair, IntWritable, Text, Text> {
	int marginal = 0;
	Text currentTerm = null;
	private Map<String, Double> stripe = new HashMap<>();

	public void reduce(Pair key, Iterator<IntWritable> count,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		if (currentTerm == null) {
			currentTerm = key.getW();
		} else if (!currentTerm.equals(key.getW())) {

			stripe.forEach((k, v) -> {
				stripe.put(k, v / marginal);

			});

			output.collect(currentTerm,
					new Text(HybridReducer.objectToString(stripe)));
			marginal = 0;
			stripe = new HashMap<String, Double>();
			currentTerm = key.getW();
		}

		double sum = 0;
		while (count.hasNext()) {
			IntWritable i = count.next();
			sum += i.get();
		}
		stripe.put(key.getU().toString(), sum);
		marginal += sum;
		currentTerm = key.getW();

	}

	public static String objectToString(Map<String, Double> result) {
		StringBuilder finalResult = new StringBuilder();
		finalResult.append("[");
		result.forEach((k, v) -> {
			finalResult.append("(" + k + "," + v + ")");
		});
		finalResult.append("]");
		System.out.println(finalResult);
		return finalResult.toString();

	}
}
