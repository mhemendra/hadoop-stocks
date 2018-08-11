package stock;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCSVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StockDataParser parser = new StockDataParser();
		boolean isValidData = parser.parse(value);
		if(isValidData){
			String stockName = parser.getStockName();
			String year = parser.getYear();
			float closingPriceFloat =parser.getClosingPrice();
			int closingPrice = Math.round(closingPriceFloat);
			Text text = new Text(stockName+"-"+year);
			context.write(text, new IntWritable(closingPrice));
		}
	}
}
