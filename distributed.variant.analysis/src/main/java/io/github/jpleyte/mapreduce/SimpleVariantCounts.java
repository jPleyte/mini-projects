package io.github.jpleyte.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * This is the main class of a MapReduce process that counts the number of times
 * each variant is seen across multiple runs.
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

		// The output key is a dbSNP id
		job.setOutputKeyClass(Text.class);

		// The output value is the number of times the variant was encountered.
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapRunIdToVariant.class);
		job.setReducerClass(ReduceVariantToCount.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		logger.info("inputPath=" + args[0] + ", outputPath=" + args[1]);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
