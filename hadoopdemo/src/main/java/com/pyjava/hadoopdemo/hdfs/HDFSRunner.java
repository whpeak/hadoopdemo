package com.pyjava.hadoopdemo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;

import com.pyjava.hadoopdemo.common.hadoop.ConfCommon;
import com.pyjava.hadoopdemo.common.hadoop.FSCommon;

public class HDFSRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		conf = ConfCommon.updateConf(conf);
		FileSystem fs = FSCommon.getFS(conf);
		
		return 0;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
