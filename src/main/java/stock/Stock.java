package stock;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stock extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Stock <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job(getConf());
		job.setJarByClass(Stock.class);
		job.setJobName("Stock Data Analysis");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StockCSVMapper.class);
		job.setReducerClass(StockClosingMaxPerYearReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Stock(), args);
		System.exit(exitCode);
	}
}
