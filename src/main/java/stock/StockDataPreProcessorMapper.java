package stock;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockDataPreProcessorMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		StockPreParser parser = new StockPreParser();
		boolean isValidData = parser.parse(value);
		if(isValidData){
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileNameWithFormat = fileSplit.getPath().getName();
			String fileName = fileNameWithFormat.substring(0,fileNameWithFormat.indexOf("."));
			Text appendedValue = new Text(fileName+","+value.toString());
			context.write(key, appendedValue);
		}
	}
}
 