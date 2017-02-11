package com.designpathshala.hadoop.MapReduce.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount {

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	        
	    Job job = new Job(conf, "word count");
	    
		job.setJarByClass(WordCount.class);
		job.setJobName("Word Count");

		 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(WordCountReducer.class);
		//Combiner
		job.setCombinerClass(WordCountReducer.class);
		job.setMapperClass(WordCountMapper.class);

		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.wordcount.WordCount /user/hue/dp/input/text.txt /user/hue/dp/output


