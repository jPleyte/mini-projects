package io.github.jpleyte.mapreduce.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import io.github.jpleyte.mapreduce.bean.Variant;

/**
 * Read asingle CSV ``Text`` line and output key=Variant with value=a count of
 * one.The Variant class must be serialised as a ``BytesWritable`` byte array
 * because Hadoop's MapReduce doesn't allow for passing of non-pojo objects.
 * 
 * @author j
 *
 */
public class MapRunIdToVariant extends Mapper<LongWritable, Text, Text, BytesWritable> {
	private static final Logger logger = Logger.getLogger(MapRunIdToVariant.class.getName());
	private CSVFormat csvFormat;

	private enum SimpleVariantCountsHeaders {
		run_id, chromosome, position_start, position_end, reference_base, variant_base, db_snp_id, significance
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {		
		super.setup(context);
		csvFormat = CSVFormat.DEFAULT.builder().setHeader(SimpleVariantCountsHeaders.class).build();
		logger.debug("completed setup");
	}
	
	/**
	 * Read a CSV line, create a variant, and Map the variant to its runId 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try(CSVParser parser = CSVParser.parse(value.toString(), csvFormat)) {
			List<CSVRecord> records = parser.getRecords();

			if (records.size() != 1) {
				throw new IllegalArgumentException("Oh no! There is supposed to be one record per line. Found "+parser.getRecords().size());
			}
			
			Variant variant = toVariant(records.get(0));
			byte[] byVriant = serialise(variant);

			context.write(new Text(variant.getRunId()), new BytesWritable(byVriant));
		}
	}

	/**
	 * Convert a CSV row to a Variant
	 * 
	 * @param csvRecord
	 * @return
	 */
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

	/**
	 * Convert a Variant to a byte array
	 * 
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	private byte[] serialise(Variant obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}

}
