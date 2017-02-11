package com.designpathshala.hadoop.MapReduce.join.ReduceSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * JoinRecordWith Userid to join user city records where they live
 * 
 * Table Users: UserId UserName 
 * 1 Jack 
 * 2 Daniel 
 * 4 Martin 
 * 5 King 
 * 3 Mary 
 * 4 Jane
 * 
 * Table City: 5	London
 * 2	London
 * 4	Rome
 * 2	Glasgow
 * 3	Paris
 * 1	Madrid
 * 
 * Join attribue: Userid and fields separated by tab.
 * 
 * */
public class Join extends Configured implements Tool {

	/**
	 * The partitioner partitions the tuples among the reducers based on the
	 * join key such that all tuples from both datasets having the same key go
	 * to the same reducer. The default partitioner had to be specifically
	 * overridden to make sure that the partitioning was done only on the Key
	 * value, ignoring the Tag value.
	 *
	 */
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("<user input> <city input> <output>");
			return -1;
		}

		Job job = Job.getInstance(new Configuration());
		job.setJobName("Join user records with city names");
		job.setJarByClass(getClass());

		Path userInputPath = new Path(args[0]);
		Path cityInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		/**
		 * Reading input from multiple places
		 */
		MultipleInputs.addInputPath(job, userInputPath, TextInputFormat.class,
				UserJoinMapper.class);
		MultipleInputs.addInputPath(job, cityInputPath, TextInputFormat.class,
				CityJoinMapper.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		/**
		 * The partitioner partitions the tuples among the reducers based on the
		 * join key such that all tuples from both datasets having the same key go
		 * to the same reducer. The default partitioner had to be specifically
		 * overridden to make sure that the partitioning was done only on the Key
		 * value, ignoring the Tag value.
		 *
		 */
		job.setPartitionerClass(KeyPartitioner.class);
		
		/**
		 *  Once the reduce function is
		 *  called, it will only get the first (key, tag) pair as its key (owing to the custom grouping
		 *  function written which ignores the tags)
		 */
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Join(), args);
		System.exit(exitCode);
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.join.ReduceSideJoin.Join /user/hue/dp/mapreduce/joins/user /user/hue/dp/mapreduce/joins/city /user/hue/dp/output