package com.pyjava.hadoopdemo.common.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FSCommon {
	
	public static FileSystem getFS(String uri,Configuration conf) throws URISyntaxException, IOException
	{
		URI hdfsURI =new URI(uri);
		FileSystem fs =FileSystem.get(hdfsURI, conf);
//		System.out.println(fs.getStatus().toString());
		return fs;
	}
	public static FileSystem getFS(Configuration conf) throws  IOException
	{
		FileSystem fs =FileSystem.get(conf);
//		System.out.println(fs.getStatus().toString());
		return fs;
	}

}
