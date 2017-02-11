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
 * When we give a position to seek() which is start of any record, 
 * then it will return the record for you.
 * But when given a value in between a record it will fail.
 * 
 * @author Design Pathshala
 *
 */
public class SequenceFileSeekTest {
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
			reader.seek(new Long(args[1]));
			/**
			 * Seeking the position which is a record boundary works as desired.
			 * However, when we seek a position which is not a record boundary,
			 * it will fail. 
			 */
			reader.next(key,value);
			System.out.printf("%s \t %s" , key, value);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fileformats.sequence.SequenceFileSeekTest /user/hue/dp/output/seqfile 3936