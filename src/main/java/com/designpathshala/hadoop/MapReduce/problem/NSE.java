package com.designpathshala.hadoop.MapReduce.problem;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NSE {

	public static class NSEMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] trade = value.toString().split(",");

			// Ignore the headers of file
			if (!trade[0].equalsIgnoreCase("SYMBOL")) {

				String series = trade[1];
				String date = trade[10];
				Text key1 = new Text(date + "," + series);

				String symbol = trade[0];
				String open = trade[2];
				Text value1 = new Text(symbol + "," + open);

				context.write(key1, value1);

			}

		}
	}

	public static class NSEReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double max = 0;
			String symbol = null;
			for (Text val : values) {
				String[] symbolOpen = val.toString().split(",");
				double open = new Double(symbolOpen[1]);
				if (max < open) {
					max = open;
					symbol = symbolOpen[0];
				}
			}

			context.write(key, new Text(symbol + "," + max));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Nse max open value for each series each day <in> <out>");
			System.exit(2);
		}

		
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf, "Nse max open for each series");

		job.setJarByClass(NSE.class);
		job.setJobName("Nse max open for each series");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(NSEMapper.class);

		job.setReducerClass(NSEReducer.class);
		// Combiner
		job.setCombinerClass(NSEReducer.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.Problem.NSE /user/hue/dp/mapreduce/nse /user/hue/dp/output

