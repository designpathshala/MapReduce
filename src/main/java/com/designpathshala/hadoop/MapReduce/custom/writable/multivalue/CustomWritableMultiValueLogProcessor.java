package com.designpathshala.hadoop.MapReduce.custom.writable.multivalue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Require the bytes transfered from an IP, as well as all the requests served from that ip.
 * 
 * 
 * Output Eg:
 * 
 * 128.187.140.171	60655	GET /cgi-bin/imagemap/countdown?375,269 HTTP/1.0	GET /images/ksclogo-medium.gif HTTP/1.0	GET /images/NASA-logosmall.gif HTTP/1.0	GET /images/MOSAIC-logosmall.gif HTTP/1.0	GET /images/USA-logosmall.gif HTTP/1.0	GET /images/WORLD-logosmall.gif HTTP/1.0	GET /shuttle/countdown/ HTTP/1.0	GET /shuttle/countdown/count.gif HTTP/1.0	GET /images/KSC-logosmall.gif HTTP/1.0	GET /cgi-bin/imagemap/countdown?97,140 HTTP/1.0	GET /ksc.html HTTP/1.0	
 * 129.188.154.200	1607516	GET /icons/image.xbm HTTP/1.0	GET /shuttle/missions/sts-71/movies/movies.html HTTP/1.0	GET /facts/about_ksc.html HTTP/1.0	GET /images/launchpalms-small.gif HTTP/1.0	GET /images/NASA-logosmall.gif HTTP/1.0	GET /images/KSC-logosmall.gif HTTP/1.0	GET /shuttle/missions/sts-71/sts-71-patch.jpg HTTP/1.0	GET / HTTP/1.0	GET /images/ksclogo-medium.gif HTTP/1.0	GET /images/MOSAIC-logosmall.gif HTTP/1.0	GET /images/USA-logosmall.gif HTTP/1.0	GET /images/WORLD-logosmall.gif HTTP/1.0	GET /shuttle/missions/sts-71/mission-sts-71.html HTTP/1.0	GET /shuttle/missions/sts-71/sts-71-patch-small.gif HTTP/1.0	GET /images/launch-logo.gif HTTP/1.0	GET /shuttle/missions/sts-71/movies/sts-71-launch.mpg HTTP/1.0	GET /shuttle/missions/sts-71/movies/movies.html HTTP/1.0	GET /shuttle/countdown/count.gif HTTP/1.0	GET /shuttle/countdown/countdown.html HTTP/1.0	GET /htbin/cdt_main.pl HTTP/1.0	GET /shuttle/countdown/lps/back.gif HTTP/1.0	GET /shuttle/countdown/lps/mpeg.html HTTP/1.0	GET /htbin/wais.pl?mpeg HTTP/1.0	GET /shuttle/movies/astronauts.mpg HTTP/1.0	GET /htbin/wais.pl HTTP/1.0	GET /shuttle/missions/sts-71/movies/movies.html HTTP/1.0	GET /shuttle/missions/sts-71/images/images.html HTTP/1.0	GET / HTTP/1.0	GET /shuttle/ HTTP/1.0	GET /shuttle/movies/ HTTP/1.0	GET /shuttle/missions/sts-71/movies/sts-71-launch.mpg HTTP/1.0	GET /shuttle/ HTTP/1.0	GET /shuttle/missions/sts-71/movies/ HTTP/1.0	GET /icons/blank.xbm HTTP/1.0	GET /icons/menu.xbm HTTP/1.0	GET /icons/movie.xbm HTTP/1.0	GET /icons/text.xbm HTTP/1.0	GET /shuttle/missions/sts-71/ HTTP/1.0	GET /shuttle/missions/ HTTP/1.0	GET /shuttle/missions/sts-71/images/ HTTP/1.0	GET /icons/image.xbm HTTP/1.0	
 * 129.193.116.41	46353	GET /shuttle/countdown/count.gif HTTP/1.0	GET /images/KSC-logosmall.gif HTTP/1.0	GET /images/NASA-logosmall.gif HTTP/1.0	GET /cgi-bin/imagemap/countdown?370,276 HTTP/1.0	GET /shuttle/countdown/ HTTP/1.0	

 * @author DP
 *
 */
public class CustomWritableMultiValueLogProcessor {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "custom-writable");

		job.setJarByClass(CustomWritableMultiValueLogProcessor.class);
		job.setMapperClass(MultiValueMap.class);
		job.setReducerClass(MultiValueReduce.class);

		/**
		 * Setting the map outvlaue class to Multivalue class
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiValueWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

// hadoop jar MapReduce-0.0.1-SNAPSHOT.jar com.designpathshala.hadoop.MapReduce.custom.writable.multivalue.CustomWritableMultiValueLogProcessor /user/hue/dp/mapreduce/access_log_Jul95 /user/hue/dp/output
