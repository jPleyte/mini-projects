package io.github.jpleyte.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce a list of Variants to the sum of the number times each was called.
 * 
 * @author j
 *
 */
public class ReduceVariantToCount extends Reducer<Text /* Variant */, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text /* Variant */ key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
	
		for (IntWritable cnt : values) {
			sum += cnt.get();
		}
		
		context.write(key, new IntWritable(sum));
	}
}
