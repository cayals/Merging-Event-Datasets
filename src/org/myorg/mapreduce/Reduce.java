package org.myorg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.SimpleDateFormat;

import org.myorg.data.CompositeKey;

import java.util.*;

/*Note: There are 4 types of record types
STATE_START
STATE_END
ATTRIBUTE_START
ATTRIBUTE_END
We want to merge the two datasets, states and attributes, so that given any segment in time, we know the state and the attribute for every item.
Assumptions:
1. Every item can only have one state at any point in time.
2. Every item has a continuous set of states.
3. Every item can only have one attribute at any point in time.*/

public class Reduce extends Reducer<CompositeKey, Text, NullWritable, Text> {

	private final String FIELD_DELIM = "\t";
	private final String FIELD_DELIM2 = ",";

	@Override
	public void reduce(CompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String line = null;
		String recordType = null;
		String lastBoundary = "2015-01-01 00:00:00";
		String currentState = "";
		String currentAttribute = "";

		for (Text val : values) {
			line = val.toString();
			String firstParts[] = line.split(FIELD_DELIM2);

			recordType = firstParts[0];
			String details = firstParts[1];

			if (recordType.equals("STATE_START")){//set the currentState, then reset lastBoundary
				String[] detailParts = details.split(FIELD_DELIM);
				if (detailParts.length >= 4) {
					String itemID = parts[0];
					String datetimeStart = parts[1];//format as "yyyy-mm-dd hh24:mm:ss"
					//String datetimeEnd = parts[2];//format as "yyyy-mm-dd hh24:mm:ss"
					String state = parts[3];

					currentState = state;
					lastBoundary = datetimeStart;
				}
			} else if (recordType.equals("STATE_END")){//write to disk, then reset currentState to empty string, then reset the lastBoundary
				String[] detailParts = details.split(FIELD_DELIM);
				if (detailParts.length >= 4) {
					String itemID = parts[0];
					//String datetimeStart = parts[1];//format as "yyyy-mm-dd hh24:mm:ss"
					String datetimeEnd = parts[2];//format as "yyyy-mm-dd hh24:mm:ss"
					String state = parts[3];

					context.write(NullWritable.get(),  new Text(new String(itemID
							+ FIELD_DELIM + lastBoundary 
							+ FIELD_DELIM + datetimeEnd
							+ FIELD_DELIM + currentState
							+ FIELD_DELIM + currentAttribute)));

					state = "";
					lastBoundary = datetimeEnd;
				}
			} else if (recordType.equals("ATTRIBUTE_START")){
				String[] detailParts = details.split(FIELD_DELIM);
				if (detailParts.length >= 4) {//write to disk, then replace the attribute, reset lastBoundary
				
					String datetimeStart = parts[0];//format as "yyyy-mm-dd hh24:mm:ss"
					//String datetimeEnd = parts[1];//format as "yyyy-mm-dd hh24:mm:ss"
					String itemID = parts[2];
					String attribute = parts[3];

					if (!lastBoundary.equals(datetimeStart)){//only write to disk if lastBoundary is different from datetimeStart, so that we don't get a 0 interval segment
						context.write(NullWritable.get(),  new Text(new String(itemID
								+ FIELD_DELIM + lastBoundary 
								+ FIELD_DELIM + datetimeStart
								+ FIELD_DELIM + currentState
								+ FIELD_DELIM + currentAttribute)));//if there isn't an attribute set, attributes = "". Note that attributes are sporadic. 
						lastBoundary = datetimeStart;
						}
						
						currentAttribute = attribute;
					
				}
			} else if (recordType.equals("ATTRIBUTE_END")){
				String[] detailParts = details.split(FIELD_DELIM);
				if (detailParts.length >= 4) {//write to disk, reset lastBoundary, reset attribute to empty string

					//String datetimeStart = parts[0];//format as "yyyy-mm-dd hh24:mm:ss"
					String datetimeEnd = parts[1];//format as "yyyy-mm-dd hh24:mm:ss"
					String itemID = parts[2];
					String attribute = parts[3];

					context.write(NullWritable.get(),  new Text(new String(itemID
							+ FIELD_DELIM + lastBoundary 
							+ FIELD_DELIM + datetimeEnd
							+ FIELD_DELIM + currentState
							+ FIELD_DELIM + currentAttribute)));

					lastBoundary = datetimeEnd;
					attribute = "";//reset
				}
			}



		}
	}
}