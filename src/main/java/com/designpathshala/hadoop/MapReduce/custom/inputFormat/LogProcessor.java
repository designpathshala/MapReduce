package com.designpathshala.hadoop.MapReduce.custom.inputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.designpathshala.hadoop.MapReduce.custom.writable.LogProcessorMap;
import com.designpathshala.hadoop.MapReduce.custom.writable.LogProcessorReduce;
import com.designpathshala.hadoop.MapReduce.inputFormat.FileInputFormatKeyValue;

/**
 * Find out the response size returned to each IP
 * @author Design Pathshala
 *
 */
public class LogProcessor {

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "custom-file-input-format");
		job.setJarByClass(FileInputFormatKeyValue.class);

		job.setJarByClass(LogProcessor.class);
		job.setMapperClass(LogProcessorMap.class);
		job.setReducerClass(LogProcessorReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(LogFileInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.custom.inputFormat/LogProcessor /user/hue/dp/mapreduce/access_log_Jul95 /user/hue/dp/output