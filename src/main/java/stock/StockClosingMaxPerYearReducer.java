package stock;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import input.IntFloat;

public class StockClosingMaxPerYearReducer extends Reducer<IntFloat, NullWritable, IntFloat, NullWritable>  {
	@Override
	public void reduce(IntFloat key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
