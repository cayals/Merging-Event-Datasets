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

public class MapEquip extends Mapper<LongWritable, Text, CompositeKey, Text> {

	private final String FIELD_DELIM = "\t";
	private final String FIELD_DELIM2 = ",";

	@Override
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String line = ivalue.toString();
		String[] parts = line.split("\t");

		if (parts.length >= 4) {
			String itemID = parts[0];
			String datetimeStart = parts[1];
			String datetimeEnd = parts[2];
			String state = parts[3];
			
			//write separate records for equip_state_out and equip_state_in
			//in ordering, if the datetime is the same, let the out states come first, denoted by the third part of the composite key

			if (!datetimeStart.equals("\\N") && !datetimeEnd.equals("\\N")){
				context.write(new CompositeKey(itemID, datetimeStart, "3"),  new Text(new String("DATASET1_START" + FIELD_DELIM2 + line)));
				context.write(new CompositeKey(itemID, datetimeEnd, "2"),  new Text(new String("DATASET1_END" + FIELD_DELIM2 + line)));


			}
		}

	}
}