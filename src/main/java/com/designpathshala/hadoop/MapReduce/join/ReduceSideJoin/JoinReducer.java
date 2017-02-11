package com.designpathshala.hadoop.MapReduce.join.ReduceSideJoin;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * JoinReducer Reducer for joining user with corresponding city
 * 
 * */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		Text userName = new Text(iter.next());
		while (iter.hasNext()) {
			Text record = iter.next();
			Text outValue = new Text(userName.toString() + "\t"
					+ record.toString());
			context.write(key.getFirst(), outValue);
		}
	}
}