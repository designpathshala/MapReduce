package com.designpathshala.hadoop.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");
		FileSystem hdfs = FileSystem.get(conf);

		Path hdfsFile = new Path(args[0]);
		String line = null;
		try {
			FSDataInputStream in = hdfs.open(hdfsFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			StringBuilder responseData = new StringBuilder();
			while ((line = br.readLine()) != null) {
				responseData.append(line);
			}
			System.out.println(responseData);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fs.ReadHDFS /user/hue/dp/input/text.txt

