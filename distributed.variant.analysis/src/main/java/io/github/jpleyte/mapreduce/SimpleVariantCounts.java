package io.github.jpleyte.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import io.github.jpleyte.mapreduce.map.MapRunIdToVariant;
import io.github.jpleyte.mapreduce.reduce.ReduceVariantToCount;

/**
 * This is the main class for a simple MapReduce job that counts the number of
 * variant calls in each run. MapReduce can't use arbitrary pojos, so the
 * Variant instance is serialised and passed as BytesWriteable.
 * 
 * @author j
 *
 */
public class SimpleVariantCounts extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(SimpleVariantCounts.class);
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new SimpleVariantCounts(), args);
		
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: SimpleVariantCounts <in_dir> <out_dir>");
			System.exit(2);
		}

		Job job = Job.getInstance(super.getConf(), SimpleVariantCounts.class.getName());

		job.setJarByClass(SimpleVariantCounts.class);

		// The Mapper maps the variant to its runId.
		job.setMapperClass(MapRunIdToVariant.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		// The reducer counts the number of variant calls in the run and prints out the
		// runId and count.
		job.setReducerClass(ReduceVariantToCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input is a line from the CSV file
		job.setInputFormatClass(TextInputFormat.class);

		// Final output is written as text to file
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		logger.info("inputPath=" + args[0] + ", outputPath=" + args[1]);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
