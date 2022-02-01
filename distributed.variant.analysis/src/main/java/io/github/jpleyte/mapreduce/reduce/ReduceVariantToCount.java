package io.github.jpleyte.mapreduce.reduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import io.github.jpleyte.mapreduce.bean.Variant;

/**
 * Take a runId and serialized list of variants. Emit the number of variants
 * that are called on the run.
 * 
 * @author j
 *
 */
public class ReduceVariantToCount extends Reducer<Text, BytesWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text runId, Iterable<BytesWritable> variants, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
	
		for (BytesWritable bytes : variants) {

			Variant variant;

			try {
				variant = deserialise(bytes.getBytes());
			} catch (ClassNotFoundException | IOException e) {
				throw new IOException("Unable to deserialise bytes to Variant: " + e.getMessage(), e);
			}
			sum += 1;
		}

		context.write(runId, new IntWritable(sum));

	}

	/**
	 * Convert the byte array to a Variant
	 * 
	 * @param data
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private Variant deserialise(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is = new ObjectInputStream(in);

		return (Variant) is.readObject();
	}
}
