package com.micron.data;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import com.micron.data.CompositeKey;

public class ActualKeyPartitioner
    extends Partitioner<CompositeKey, Text> {

   /*  @Override
    public void configure(JobConf job) {} */
    
    @Override
    public int getPartition(CompositeKey key, Text value, int numPartitions) {
      return Math.abs((key.getfirst_key().hashCode()) * 127) % numPartitions;
    }
  }