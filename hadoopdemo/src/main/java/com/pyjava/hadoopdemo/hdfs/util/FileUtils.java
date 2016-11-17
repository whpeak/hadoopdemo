package com.pyjava.hadoopdemo.hdfs.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils {
	
	public static List<String> listFiles(FileSystem fs,String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		List<String>  files= new ArrayList<String>();
		FileStatus[] status = fs.listStatus(new Path(path));
		for (FileStatus file : status) 
		{
			files.add(file.getPath().toString());
		}
		return files;
		
	}

}
