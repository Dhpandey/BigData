package hybrid.relative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HybridMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Pair, IntWritable> {
	Pair p1 = new Pair();

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
					output.collect(new Pair(list.get(i), list.get(j)),
							new IntWritable(1));
				} else
					break;
			}
		}
	//	output.collect(new Pair("zzzzz", "zzzzzz"), new IntWritable(1));
	}
}
