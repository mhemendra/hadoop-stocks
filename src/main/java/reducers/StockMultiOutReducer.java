package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import input.IntFloat;

public class StockMultiOutReducer extends Reducer<IntFloat, Text, IntFloat, NullWritable>  {

	private MultipleOutputs<IntFloat, NullWritable> multipleOutputs;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	 multipleOutputs = new MultipleOutputs<IntFloat, NullWritable>(context);	
	}
	
	@Override
	public void reduce(IntFloat key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String stockName=null;
		Iterator<Text> iter= values.iterator();
		while(iter.hasNext()){
			stockName=iter.next().toString();
			break;
		}
		multipleOutputs.write(key, NullWritable.get(),stockName);
	}	
}
