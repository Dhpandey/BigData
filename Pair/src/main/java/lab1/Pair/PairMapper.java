package lab1.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PairMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Pair, IntWritable> {

	public void map(LongWritable key, Text value,
			OutputCollector<Pair, IntWritable> output, Reporter r)
			throws IOException {

		String values = value.toString();
		List<String> list = new ArrayList<String>();
		for (String val : values.split(",")) {
			list.add(val);
		}

		for (int i = 0; i < list.size() - 1; i++) {
			for (int j = i + 1; j < list.size(); j++) {
				if (!(list.get(i).equalsIgnoreCase(list.get(j)))) {
					String mapElement = list.get(i);
					output.collect(new Pair(mapElement, list.get(j)),
							new IntWritable(1));
					output.collect(new Pair(mapElement, "*"),
							new IntWritable(1));
				} else
					break;
			}
		}
	}
}
