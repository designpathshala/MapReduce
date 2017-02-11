package com.designpathshala.hadoop.MapReduce.problem;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WikiSurvey {

	public static class WikiMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] wiki = value.toString().split(",");
			IntWritable score = new IntWritable();
			Text ideaText = new Text(wiki[2]);
			score.set(new Integer(wiki[3]) - new Integer(wiki[4]));

			context.write(ideaText, score);
		}
	}

	public static class WikiReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			IntWritable result = new IntWritable();
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum > 0){
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wiki survey best idea text <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Wiki best idea");

		job.setJarByClass(WikiSurvey.class);
		job.setJobName("Wiki best idea");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WikiMapper.class);

		job.setReducerClass(WikiReducer.class);
		// Combiner
		job.setCombinerClass(WikiReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.Problem.WikiSurvey /user/hue/dp/mapreduce/wikisurvey /user/hue/dp/output

