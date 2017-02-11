package com.designpathshala.hadoop.fileformats.sequence;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Writes a sequence file eg:
 * 100	One, tow, buckel my show
 * 99	Three, four, shut the door
 * 
 * We write the key and value to the console, along with the position.
 * [128] 	100	One, tow, buckel my show
 * [173]	99	Three, four, shut the door
 * 
 * @author Design Pathshala
 *
 */
public class SequenceFileWrite {
	private static final String[] DATA = { "One, two, buckle my shoe",
			"Three, four, shut the door", "Five, six, pick up sticks",
			"Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
					value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
						value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}

//hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fileformats.sequence.SequenceFileWrite /user/hue/dp/output/seqfile 	
