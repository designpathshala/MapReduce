package com.designpathshala.hadoop.MapReduce.problem.project;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.designpathshala.hadoop.MapReduce.custom.writable.LogWritable;


public class ForumDataInputFormat extends FileInputFormat<LongWritable, LogWritable>{

	@Override
	public RecordReader<LongWritable, LogWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
				return null;
//		return new ForumDataRecordReader();
	}

}
