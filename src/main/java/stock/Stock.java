package stock;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import comparators.GroupComparator;
import comparators.KeyComparator;
import input.IntFloat;

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
	    job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(StockCSVMapper.class);
		job.setReducerClass(StockMultiOutReducer.class);
		job.setOutputKeyClass(IntFloat.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Stock(), args);
		System.exit(exitCode);
	}
}
