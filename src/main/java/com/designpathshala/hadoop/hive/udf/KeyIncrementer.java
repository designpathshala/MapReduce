package com.designpathshala.hadoop.hive.udf;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

@Description(name = "rand",
		    value = "_FUNC_() - Returns a current Time in millisecond + last digits of ip address")
@UDFType(deterministic = false)
public class KeyIncrementer extends UDF{

	public LongWritable evaluate() {
		LongWritable key = new LongWritable();
		String ipaddress;
		Double random;
		String randomString;
		long time;
//		try {
	 
//			ipaddress = InetAddress.getLocalHost().getHostAddress();
//			ipaddress = ipaddress.substring(ipaddress.lastIndexOf('.')+1);
//			if(ipaddress.length() ==1){
//				ipaddress = "00" + ipaddress;
//			}else if(ipaddress.length() == 2){
//				ipaddress = "0" + ipaddress;
//			}else if(ipaddress.length() == 0){//In case of error
//				ipaddress = "000";
//			}
			time = System.nanoTime();
			
			int length = 19;
			String timeString = new Long(time).toString();
			int timeLenght = timeString.length();
			int randomMultipluier = 1;
			for(int i=0;i< length-timeLenght; i++){
				randomMultipluier = randomMultipluier*10;
			}

			random = Math.random()*randomMultipluier;
			randomString = new Long(random.longValue()).toString();
			
			while(randomString.length() < length-timeLenght){
				randomString = "0"+ randomString;
			}
			
			key.set(Long.valueOf(time + randomString));
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//			key.set((long) (Math.random()*100000000));
//		}
		return key;
	}
	
	public static void main(String[] args){
		KeyIncrementer ki = new KeyIncrementer();
		System.out.println(ki.evaluate());
	}
	
}
