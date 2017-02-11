package com.designpathshala.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeLocalPutToHDFS {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);

		Path inputDir = new Path(args[0]);
		Path hdfsFile = new Path(args[1]);

		try {
			FileStatus[] inputFiles = local.listStatus(inputDir);
			FSDataOutputStream out = hdfs.create(hdfsFile);

			for (int i = 0; i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}

//  hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.fs.MergeLocalPutToHDFS /usr/lib/hue/desingpathshala/datafiles /user/hue/dp/input/inputfile.txt

