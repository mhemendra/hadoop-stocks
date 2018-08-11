package stock;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class StockMultiOutReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable>  {

	private MultipleOutputs<IntWritable, IntWritable> multipleOutputs;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	 multipleOutputs = new MultipleOutputs<IntWritable, IntWritable>(context);	
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String[] keyArray = key.toString().split("-");
		String stockName=keyArray[0];
		String year=keyArray[1];
		Iterator<IntWritable> iterator = values.iterator();
		int maxClosing = 0;
		while (iterator.hasNext()) {
			int iterValue=iterator.next().get();
			if(iterValue>maxClosing){
				maxClosing=iterValue;
			}
		}
		multipleOutputs.write(new IntWritable(Integer.parseInt(year)), new IntWritable(maxClosing),stockName);
	}	
}
