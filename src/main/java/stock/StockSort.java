package stock;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.StockSortMapper;
import reducers.StockSortReducer;

public class StockSort extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Stock <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job(getConf());
		job.setJarByClass(StockSort.class);
		job.setJobName("Stock Sort");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StockSortMapper.class);
		job.setReducerClass(StockSortReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<FloatWritable, IntWritable> sampler = new InputSampler.RandomSampler<FloatWritable, IntWritable>(
				0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StockSort(), args);
		System.exit(exitCode);
	}
}
