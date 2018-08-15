package stock;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.StockSortMapper;

public class StockSortPreProcessor extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Stock <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job(getConf());
		job.setJarByClass(StockSortPreProcessor.class);
		job.setJobName("Stock Sort");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StockSortMapper.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StockSortPreProcessor(), args);
		System.exit(exitCode);
	}
}
