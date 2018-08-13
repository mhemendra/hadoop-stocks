package input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntFloat implements WritableComparable<IntFloat> {
	private int year;
	private float maxClosing;

	public IntFloat() {
	};

	public IntFloat(int year, float maxClosing) {
		set(year, maxClosing);
	}

	private void set(int year, float maxClosing) {
		this.year = year;
		this.maxClosing = maxClosing;

	}

	public int getYear() {
		return year;
	}

	public void setFirstInt(int year) {
		this.year = year;
	}

	public float getMaxClosing() {
		return maxClosing;
	}

	public void setMaxClosing(int maxClosing) {
		this.maxClosing = maxClosing;
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		maxClosing = in.readInt();

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeFloat(maxClosing);

	}

	@Override
	public int hashCode() {
		return year * 163;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntFloat) {
			IntFloat ip = (IntFloat) o;
			return year == ip.year && maxClosing == ip.maxClosing;
		}
		return false;
	}

	public int compareTo(IntFloat ip) {
		int firstComp = compare(year, ip.year);
		if (firstComp != 0) {
			return firstComp;
		} else {
			return compare(maxClosing, ip.maxClosing);
		}

	}

	public static int compare(int a, int b) {
		return (a < b ? -1 : (a == b ? 0 : 1));
	}

	public static int compare(float a, float b) {
		return (a < b ? -1 : (a == b ? 0 : 1));
	}
	
	@Override
	public String toString() {
		return year + "\t" + maxClosing;
	}
}
