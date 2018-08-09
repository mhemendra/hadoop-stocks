package stock;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StockClosingMaxPerYearReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>  {
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		int maxClosing = 0;
		while (iterator.hasNext()) {
			int iterValue=iterator.next().get();
			if(iterValue>maxClosing){
				maxClosing=iterValue;
			}

		}
		context.write(key, new IntWritable(maxClosing));

	}
}
