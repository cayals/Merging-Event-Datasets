package org.myorg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.myorg.data.ActualKeyGroupingComparator;
import org.myorg.data.ActualKeyPartitioner;
import org.myorg.data.CompositeKey;
import org.myorg.data.CompositeKeyComparator;

public class Driver extends Configured implements Tool {

@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapAttribute.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapState.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(org.myorg.mapreduce.Reduce.class);
		
		//for debugging
		//job.setNumReduceTasks(0);
		//job.setMaxMapAttempts(100);
		//job.setMaxReduceAttempts(100);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setPartitionerClass(ActualKeyPartitioner.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1 ;
	}

  
   public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Driver(), args);
    System.exit(exitCode);
  }
  
  
}