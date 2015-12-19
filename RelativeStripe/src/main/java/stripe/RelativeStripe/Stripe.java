package stripe.RelativeStripe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stripe implements Writable {

	private Text neighbour = new Text();
	private IntWritable count = new IntWritable();

	public Stripe() {
		// TODO Auto-generated constructor stub
	}

	public Stripe(Text neighbour, IntWritable count) {
		this.neighbour = neighbour;
		this.count = count;

	}

	public Stripe(String neighbour, int count) {
		this.neighbour = new Text(neighbour);
		this.count = new IntWritable(count);
	}

	public void write(DataOutput out) throws IOException {
		this.neighbour.write(out);
		this.count.write(out);
	}

	public void readFields(DataInput in) throws IOException {

		this.neighbour.readFields(in);
		this.count.readFields(in);
	}

}
