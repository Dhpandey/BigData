package lab1.Pair;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Needed both input and output directory");
			return -1;
		}
		JobConf conf = new JobConf(PairDriver.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(PairMapper.class);
		conf.setReducerClass(PairReducer.class);

		conf.setMapOutputKeyClass(Pair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Pair.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PairDriver(), args);
		System.exit(exitCode);
	}

}
