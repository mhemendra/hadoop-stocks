package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StockSortReducer extends Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable>  {
	@Override
	public void reduce(FloatWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		IntWritable year = null;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()){
			year = iter.next();
			break;
		}
		context.write(key, year);
	}
}
