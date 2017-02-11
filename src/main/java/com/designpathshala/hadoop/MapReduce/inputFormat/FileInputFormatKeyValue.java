package com.designpathshala.hadoop.MapReduce.inputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FileInputFormatKeyValue {

public static class MyMapper extends Mapper<Text, Text, Text, Text> {
		
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			 context.write(key, value);
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// process values
			for (Text value : values) {
				 context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			JobConf conf = new JobConf();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 2) {
	            System.err.println("Usage: <in> <out>");
	            System.exit(2);
	        }

	        Job job = new Job(conf, "FileInputFormatKeyValue");
	        job.setJarByClass(FileInputFormatKeyValue.class);

	        job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
		
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.getConfiguration().set("key.value.separator.in.input.line", ",");
			
			KeyValueTextInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);


			 System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.inputFormat.FileInputFormatKeyValue /user/hue/dp/input/keyvalueinput.txt /user/hue/dp/output
