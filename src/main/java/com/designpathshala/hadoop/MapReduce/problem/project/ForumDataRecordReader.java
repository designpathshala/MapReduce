package com.designpathshala.hadoop.MapReduce.problem.project;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.designpathshala.hadoop.MapReduce.custom.writable.LogWritable;


public class ForumDataRecordReader  extends RecordReader<ForumDataWritable, LogWritable>{

	LineRecordReader lineReader;
	ForumDataWritable value;
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext attempt)
			throws IOException, InterruptedException {
		lineReader = new LineRecordReader();
		lineReader.initialize(inputSplit, attempt);		
				
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String record = "";
		
		if (!lineReader.nextKeyValue())
		{
			return false;
		}
		
		String currLine = lineReader.getCurrentValue().toString();
		boolean process = true;
		
		//Reach the start of record
		while(!currLine.startsWith("\"")){
			if (!lineReader.nextKeyValue())
			{
				return false;
			}
			currLine = lineReader.getCurrentValue().toString();
		}
		
		//Read the record
		do{
			record = record + currLine;
			if (!lineReader.nextKeyValue())
			{
				return false;
			}
			currLine = lineReader.getCurrentValue().toString();
		}while((!currLine.startsWith("\"")));
		
		
		
		return true;
	}
	
	
	private ForumDataWritable process(String record){
		
//		
//		String userIP = matcher.group(1);
//		String timestamp = matcher.group(4);
//		String request = matcher.group(5);
//		int status = Integer.parseInt(matcher.group(6));
//		int bytes = Integer.parseInt(matcher.group(7));
		
		return value = new ForumDataWritable();
	}
	
	@Override
	public ForumDataWritable getCurrentKey() throws IOException,
			InterruptedException {
				return value;
//		return lineReader.getCurrentKey();
	}

	@Override
	public LogWritable getCurrentValue() throws IOException,
			InterruptedException {
				return null;
//		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();		
	}

}
