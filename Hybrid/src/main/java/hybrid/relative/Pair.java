package hybrid.relative;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text w = new Text();
	private Text u = new Text();

	public Pair() {

	}

	public Pair(Text w, Text u) {
		super();
		this.w = w;
		this.u = u;
	}

	public Pair(String w, String u) {
		super();
		this.w = new Text(w);
		// System.out.println(this.w);
		this.u = new Text(u);
		// System.out.println(this.u);
	}

	public Text getW() {
		return w;
	}

	public void setW(Text w) {
		this.w = w;
	}

	public Text getU() {
		return u;
	}

	public void setU(Text u) {
		this.u = u;
	}

	@Override
	public String toString() {
		return "("+w + "," + u+")";

	}

	public void write(DataOutput out) throws IOException {
		this.w.write(out);
		this.u.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.w.readFields(in);
		this.u.readFields(in);

	}

	public int compareTo(Pair p) {
		int result = this.w.compareTo(p.w);
		if (result == 0) {
			result = this.u.compareTo(p.u);
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair)) {
			return false;
		}
		Pair pairObj = (Pair) obj;
		// System.out.println(this.w);
		// System.out.println(this.u);
		return this.w.equals(pairObj.w) && this.u.equals(pairObj.u);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((w == null) ? 0 : w.hashCode());
		result = prime * result + ((u == null) ? 0 : u.hashCode());

		return result;
	}

}
