package com.designpathshala.hadoop.MapReduce.jobdependency;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.designpathshala.hadoop.MapReduce.custom.inputFormat.LogFileInputFormat;
import com.designpathshala.hadoop.MapReduce.custom.writable.LogProcessorMap;
import com.designpathshala.hadoop.MapReduce.custom.writable.LogProcessorReduce;

public class JobController {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		/* input parameters */
		String inputPath = otherArgs[0];	
		String outputPath = otherArgs[1];
		String intPath = outputPath + "Job1";
		

		Job job1 = new Job(conf, "log-grep");
		job1.setJarByClass(RegexMapper.class);
		job1.setMapperClass(RegexMapper.class);	
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(intPath));		
		job1.setNumReduceTasks(0);	

		Job job2 = new Job(conf, "log-analysis");
		job2.setJarByClass(LogProcessorMap.class);
		job2.setMapperClass(LogProcessorMap.class);
		job2.setReducerClass(LogProcessorReduce.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    job2.setInputFormatClass(LogFileInputFormat.class);	    
		FileInputFormat.setInputPaths(job2, new Path(intPath+"/part*"));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));		
		
		
		ControlledJob controlledJob1 =  new ControlledJob(job1.getConfiguration());
		ControlledJob controlledJob2 =  new ControlledJob(job2.getConfiguration());
		controlledJob2.addDependingJob(controlledJob1);
		
		JobControl jobControl = new JobControl("JobControlDemoGroup");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		
		while (!jobControl.allFinished()){
			Thread.sleep(500);
		}
	
		jobControl.stop();
	}
}
