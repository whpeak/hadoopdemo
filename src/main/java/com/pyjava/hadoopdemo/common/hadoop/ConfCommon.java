package com.pyjava.hadoopdemo.common.hadoop;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class ConfCommon {
	public static Configuration updateConf(Configuration conf)
	{
//		ConfCommon.printConf(conf);
//		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//		conf.setBoolean("mapred.compress.map.output", false); 
//		conf.setBoolean("mapreduce.map.output.compress", false); 
//		conf.unset("mapred.map.output.compress.codec");
//		conf.unset("mapreduce.map.output.compress.codec");
		return conf;
	}
	
	public static void printConf(Configuration conf)
	{
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while(iterator.hasNext())
		{
			System.out.println(iterator.next());
		}
	}
	

}
