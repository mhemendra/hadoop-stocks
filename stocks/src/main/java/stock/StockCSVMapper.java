package stock;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCSVMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int dayofWeek;
		if (!line.contains("Date")) {
			String[] columns = line.split(",");
			String year = columns[0].split("-")[0];
			float closingPriceFloat =Float.parseFloat(columns[5]);
			int closingPrice = Math.round(closingPriceFloat);
			context.write(new IntWritable(Integer.parseInt(year)), new IntWritable(closingPrice));
		}
	}
}
