package stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.StockDataPreProcessorMapper;


public class StockDataPreProcessor extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Stock <input path> <output path>");
			System.exit(-1);
		}
		Configuration conf = getConf();
		Job job = new Job(conf);		
		conf.set("mapred.textoutputformat.separatorText", ",");
		job.setJarByClass(StockDataPreProcessor.class);
		job.setJobName("Stock Data Pre Processor");
		job.setMapperClass(StockDataPreProcessorMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, false);
		return job.waitForCompletion(true) ? 0: 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new StockDataPreProcessor(), args);
		System.exit(exitCode);
	}

}
