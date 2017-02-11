package com.designpathshala.hadoop.MapReduce.custom.writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Custom Writable
 * @author Design Pathshala
 *
 */
public class LogWritable implements WritableComparable<LogWritable> {

	private Text userIP, timestamp, request;	
	private IntWritable responseSize, status;	

	public LogWritable() {
		this.userIP = new Text();
		this.timestamp =  new Text();
		this.request = new Text();
		this.responseSize = new IntWritable();
		this.status = new IntWritable();		
	}
	
	public void set (String userIP, String timestamp, String request, int bytes, int status)
	{
		this.userIP.set(userIP);
		this.timestamp.set(timestamp);
		this.request.set(request);
		this.responseSize.set(bytes);
		this.status.set(status);	
	}
	

	/**
	 * Use for de-serializing the data from the DataInput object.
	 * It is also possible to use java primitive data types as the fields fo the Writable type
	 * and use the corresponding read methods of DataInput object
	 * int responseSize = in.readInt();
	 * String userIP = in.readUTF();
	 */
	public void readFields(DataInput in) throws IOException {
		userIP.readFields(in);
		timestamp.readFields(in);
		request.readFields(in);
		responseSize.readFields(in);
		status.readFields(in);
	}

	/**
	 * Serialize the objects.
	 * Java primitive data types as the files of the Writable object
	 * out.writeInt(responseSize);
	 * out.writeUTF(userIP);
	 */
	public void write(DataOutput out) throws IOException {
		userIP.write(out);
		timestamp.write(out);
		request.write(out);
		responseSize.write(out);
		status.write(out);
	}
	
	/**
	 * Compare for Keys
	 */
	public int compareTo(LogWritable o) {
		if (userIP.compareTo(o.userIP) == 0) {
			return timestamp.compareTo(o.timestamp);
		} else
			return userIP.compareTo(o.userIP);
	}
	
	public int hashCode()
	{
		return userIP.hashCode();
	}

	public Text getUserIP() {
		return userIP;
	}


	public Text getTimestamp() {
		return timestamp;
	}


	public Text getRequest() {
		return request;
	}


	public IntWritable getResponseSize() {
		return responseSize;
	}


	public IntWritable getStatus() {
		return status;
	}



}
