package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import input.IntFloat;
import parsers.StockDataParser;

public class StockCSVMapper extends Mapper<LongWritable, Text, IntFloat, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StockDataParser parser = new StockDataParser();
		boolean isValidData = parser.parse(value);
		if(isValidData){
			context.write(new IntFloat(parser.getYear(),parser.getClosingPrice()), new Text(parser.getStockName()));
		}
	}
}
