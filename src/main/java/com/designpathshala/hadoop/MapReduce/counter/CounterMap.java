package com.designpathshala.hadoop.MapReduce.counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.designpathshala.hadoop.MapReduce.custom.writable.LogWritable;

public class CounterMap extends Mapper<Object, LogWritable, Text, IntWritable > {

	public static enum LOG_PROCESSOR_COUNTER {
		  BAD_RECORDS,
		  PROCCESSED_RECORDS
		};

		// Uncomment the following to execute the DistributedCache example
/*	
    Path[] localCachePath;

	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		localCachePath = DistributedCache.getLocalCacheArchives(conf);
		
		File lookupDbDir = new File("ip2locationdb");
		String[] children = lookupDbDir.list();
		
		if (children == null) {
		    System.out.println("Cached archive directory is empty!!");
		} else {
		    for (int i=0; i<children.length; i++) {		       
		        System.out.println(children[i]);
		    }
		}
	}
*/
	
	public void map(Object key, LogWritable value, Context context)
			throws IOException, InterruptedException {		
		context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1L);

		// make bytes longWritable and output two value types...
		context.write(value.getUserIP(),value.getResponseSize());
	}
}
