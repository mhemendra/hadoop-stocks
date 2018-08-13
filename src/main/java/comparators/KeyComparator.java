package comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import input.IntFloat;

public class KeyComparator extends WritableComparator {
	protected KeyComparator() {
		super(IntFloat.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntFloat int1 = (IntFloat) w1;
		IntFloat int2 = (IntFloat) w2;
		int firstCmp=IntFloat.compare(int1.getYear(), int2.getYear());
		if(firstCmp!=0){
			return firstCmp;
		}
		return IntFloat.compare(int1.getMaxClosing(), int2.getMaxClosing());
		
	}
}

