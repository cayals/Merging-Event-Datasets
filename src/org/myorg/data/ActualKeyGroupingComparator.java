package com.micron.data;

import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator extends WritableComparator {

	protected ActualKeyGroupingComparator() {

		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		/* // (check on udid) */
		return (key1.getfirst_key()).compareTo(key2.getfirst_key());
	}
}