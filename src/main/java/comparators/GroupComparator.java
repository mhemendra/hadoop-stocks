package comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import input.IntFloat;

public class GroupComparator extends WritableComparator {
	protected GroupComparator() {
		super(IntFloat.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntFloat int1 = (IntFloat) w1;
		IntFloat int2 = (IntFloat) w2;
		return IntFloat.compare(int1.getYear(), int2.getYear());
	}
}

