package com.designpathshala.hadoop.MapReduce.inbuldMappers;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InverseInput {

public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// process values
			for (Text value : values) {
				 context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "InverseInput");
        job.setJarByClass(InverseInput.class);

		job.setMapperClass(InverseMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.getConfiguration()
				.set("key.value.separator.in.input.line", ",");
		KeyValueTextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		 System.exit(job.waitForCompletion(true) ? 0 : 1);

		JobClient.runJob(conf);
	}
}

//  hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.inbuldMappers.InverseInput /user/hue/dp/input/keyvalueinput.txt /user/hue/dp/output

