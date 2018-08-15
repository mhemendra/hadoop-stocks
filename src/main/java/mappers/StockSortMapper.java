package mappers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import parsers.StockDataParser;

public class StockSortMapper extends Mapper<LongWritable, Text, FloatWritable, IntWritable>{
	
@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	StockDataParser parser = new StockDataParser();
	boolean isValidData = parser.parse(value);
	if(isValidData){
		context.write(new FloatWritable(parser.getHighestPrice()), new IntWritable(parser.getYear()));
	}
}

}
