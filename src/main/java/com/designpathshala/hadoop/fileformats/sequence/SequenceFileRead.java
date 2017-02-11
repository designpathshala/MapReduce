package com.designpathshala.hadoop.fileformats.sequence;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Iterate over records of File created in SequenceFileWrite by using next() method
 * 
 * @author Design Pathshala
 *
 */
public class SequenceFileRead {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();
			/**
			 * next() retruns the value true.
			 * if key-value pair was read else false if the end of file
			 */
			while (reader.next(key, value)) {
				/**
				 * Prints if the sync poit is seen with the record.
				 * sync point is a point in the stream that can be used to re-synchronize
				 * with a record boundary if the reader is lost.
				 * Such entries are small enough to incur only a modest storage overhead - less than 1%.
				 * Sync point always align with the record boundaries. 
				 */
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
						value);
				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fileformats.sequence.SequenceFileRead /user/hue/dp/output/seqfile
//Hadoop command to read the file:  hadoop fs -text /user/hue/dp/output