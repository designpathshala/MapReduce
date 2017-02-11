package com.designpathshala.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListFilesHDFS {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");
		FileSystem hdfs = FileSystem.get(conf);

		Path hdfsFile = new Path(args[0]);
		try {
			FileStatus[] status = hdfs.listStatus(hdfsFile);
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for(Path p : listedPaths){
				System.out.println(p);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fs.ListFilesHDFS /user/hue/dp/input/input.txt

