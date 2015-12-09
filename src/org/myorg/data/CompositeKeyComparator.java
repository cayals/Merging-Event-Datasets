package com.micron.data;


import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.io.WritableComparator;

import com.micron.data.CompositeKey;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
      super(CompositeKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      CompositeKey key1 = (CompositeKey) w1;
      CompositeKey key2 = (CompositeKey) w2;
      int cmp = (key1.getfirst_key()).compareTo(key2.getfirst_key());
      if (cmp == 0) {
		int cmp2 = (key1.getsecond_key()).compareTo(key2.getsecond_key());
		if (cmp2 == 0) {
			return (key1.getthird_key()).compareTo(key2.getthird_key());
		}
		return cmp2;
	  }
	  return cmp; //reverse
    }
  }