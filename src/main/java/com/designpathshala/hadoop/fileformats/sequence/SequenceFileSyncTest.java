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
 * Using the sync() to synch with the record boundary, if the reader is lost.
 * 
 * @author Design Pathshala
 *
 */
public class SequenceFileSyncTest {
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
			/**
			 * Sync() is used to sync the lost reader
			 */
			reader.sync(new Long(args[1]));
			System.out.println("Reader postion is: " + reader.getPosition());
			reader.next(key,value);
			System.out.printf("%s \t %s" , key, value);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fileformats.sequence.SequenceFileSyncTest /user/hue/dp/output 3931