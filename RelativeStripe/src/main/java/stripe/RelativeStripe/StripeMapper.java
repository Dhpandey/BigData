package stripe.RelativeStripe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StripeMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, MapWritable> {

	public void map(LongWritable id, Text data,
			OutputCollector<Text, MapWritable> output, Reporter arg3)
			throws IOException {

		List<String> items = new ArrayList<String>();
		String value = data.toString();
		
		for (String item : value.split(",")) {
			items.add(item);
		}
		
		for (int i = 0; i < items.size() - 1; i++) {
			HashMap<Text, IntWritable> map = new HashMap<Text, IntWritable>();

			for (int j = i + 1; j < items.size(); j++) {
				if (!items.get(i).toString()
						.equalsIgnoreCase(items.get(j).toString())) {

					if (!map.containsKey(new Text(items.get(j)))) {
						map.put(new Text(items.get(j)), new IntWritable(1));
					} else {
						map.put(new Text(items.get(j)), new IntWritable(map
								.get(new Text(items.get(j))).get() + 1));
					}
				} else
					break;
			}

			MapWritable mw = new MapWritable();
			Iterator entries = map.entrySet().iterator();
			while (entries.hasNext()) {
				Map.Entry entry = (Map.Entry) entries.next();
				Writable key = (Writable) entry.getKey();
				Writable value1 = (Writable) entry.getValue();
				mw.put(key, value1);
			}
			output.collect(new Text(items.get(i)), mw);
		}

	}
}
