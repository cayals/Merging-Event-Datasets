package org.myorg.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.myorg.data.CompositeKey;

/*Notes: Idea is to split processing by itemID.
The input to this mapper is DATASET1, with each record having state, with start and end times.
For each record, we generate pass two records to the reducer, one for the start time, and one for the end time.*/

public class MapLot extends Mapper<LongWritable, Text, CompositeKey, Text> {

	private final String FIELD_DELIM = "\t";
	private final String FIELD_DELIM2 = ",";

	@Override
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String line = ivalue.toString();
		String[] parts = line.split(FIELD_DELIM);

		if (parts.length >= 4) {
			String datetimeStart = parts[0];//format as "yyyy-mm-dd hh24:mm:ss"
			String datetimeEnd = parts[1];//format as "yyyy-mm-dd hh24:mm:ss"
			String itemID = parts[2];
			String attribute = parts[3];*/

				context.write(new CompositeKey(itemID, segment_start, "4"),  new Text(new String("ATTRIBUTE_START" + FIELD_DELIM2 + line)));
				context.write(new CompositeKey(itemID, segment_end, "1"),  new Text(new String("ATTRIBUTE_END" + FIELD_DELIM2 + line)));
			
		}

	}
}