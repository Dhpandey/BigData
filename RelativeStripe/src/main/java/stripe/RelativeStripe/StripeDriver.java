package stripe.RelativeStripe;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StripeDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Needed both input and output directory");
			return -1;
		}
		JobConf conf = new JobConf(StripeDriver.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(StripeMapper.class);
		conf.setReducerClass(StripeReducer.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(MapWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(MapWritable.class);

		JobClient.runJob(conf); // submit the job

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StripeDriver(), args);
		System.exit(exitCode);
	}

}
