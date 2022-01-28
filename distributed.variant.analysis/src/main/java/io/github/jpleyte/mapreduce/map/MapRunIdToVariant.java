package io.github.jpleyte.mapreduce.map;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import io.github.jpleyte.mapreduce.bean.Variant;

/**
 * Map a CSV Text line to key=Variant, value=1
 * 
 * @author j
 *
 */
public class MapRunIdToVariant extends Mapper<LongWritable, Text, Text/* Variant */, IntWritable> {
	private static final Logger logger = Logger.getLogger(MapRunIdToVariant.class.getName());

	private static AtomicInteger jDebugInt = new AtomicInteger();
	private CSVFormat csvFormat;

	private enum SimpleVariantCountsHeaders {
		run_id, chromosome, position_start, position_end, reference_base, variant_base, db_snp_id, significance
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text/* Variant */, IntWritable>.Context context)
			throws IOException, InterruptedException {		
		super.setup(context);
		csvFormat = CSVFormat.DEFAULT.builder().setHeader(SimpleVariantCountsHeaders.class).build();
		logger.debug("completed setup");
	}
	
	/**
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		logger.debug("jDebug: key=" + key.get());

		logger.error("jDebug: int=" + jDebugInt.incrementAndGet() + ", value=" + value.toString());

		try(CSVParser parser = CSVParser.parse(value.toString(), csvFormat)) {
			List<CSVRecord> records = parser.getRecords();

			if (records.size() != 1) {
				throw new IllegalArgumentException("Oh no! There is supposed to be one record per line. Found "+parser.getRecords().size());
			}
			
			Variant variant = toVariant(records.get(0));
			context.write(new Text(variant.getDbSnpId()), new IntWritable(1));
		}
	}
	
	private Variant toVariant(CSVRecord csvRecord) {
		Variant variant = new Variant();
		variant.setRunId(csvRecord.get(SimpleVariantCountsHeaders.run_id));
		variant.setChromosome(csvRecord.get(SimpleVariantCountsHeaders.chromosome));
		variant.setPositionStart(Integer.valueOf(csvRecord.get(SimpleVariantCountsHeaders.position_start)));
		variant.setPositionEnd(Integer.valueOf(csvRecord.get(SimpleVariantCountsHeaders.position_end)));
		variant.setReferenceBase(csvRecord.get(SimpleVariantCountsHeaders.reference_base));
		variant.setVariantBase(csvRecord.get(SimpleVariantCountsHeaders.variant_base));
		variant.setDbSnpId(csvRecord.get(SimpleVariantCountsHeaders.db_snp_id));
		variant.setSignificance(csvRecord.get(SimpleVariantCountsHeaders.significance));		
		return variant;
	}		
}
