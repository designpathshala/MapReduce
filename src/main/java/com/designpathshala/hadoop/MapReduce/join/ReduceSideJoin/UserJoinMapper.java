package com.designpathshala.hadoop.MapReduce.join.ReduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * JoinRecordMapper Mapper for tagging weather records for a reduce-side join
 * 
 * */
public class UserJoinMapper extends
		Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (!line.startsWith("#")) {
			String[] fields = line.split("\t");
			if (fields.length == 2) {
				context.write(new TextPair(fields[0], "0"),new Text( fields[1]));
			}
		}
	}

}