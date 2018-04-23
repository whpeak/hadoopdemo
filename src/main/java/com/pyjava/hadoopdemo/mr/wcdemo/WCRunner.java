package com.pyjava.hadoopdemo.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.pyjava.hadoopdemo.common.hadoop.ConfCommon;
import com.pyjava.hadoopdemo.common.hadoop.FSCommon;

public class WCRunner extends Configured implements Tool{
	/**
	 * 创建job
	 * step1: 设置job的mapper
	 * step2:设置mapper的输出
	 * 
	 * step3:设置job的reduce
	 * step4:设置reduce的输出
	 * @param conf
	 * @param args
	 * @return
	 * @throws IOException
	 */
private Job createJob(Configuration conf,String[] args) throws IOException
{
	Job job =Job.getInstance(conf);
	conf=ConfCommon.updateConf(conf);
	FileSystem fs = FSCommon.getFS(conf);
	String hdfs_url=fs.getUri().toString();
	String input_path=hdfs_url+"/Users/wangheng/Documents/fileIO/input1";
	System.out.println(input_path);
	String output_path=hdfs_url+"/Users/wangheng/Documents/fileIO/output1";
	System.out.println(output_path);
//	job.setJarByClass(WCRunner.class);
	job.setMapperClass(WCMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	
	job.setReducerClass(WCReducer.class);
	job.setOutputKeyClass(Text.class);  
    job.setOutputValueClass(Text.class);  
    
    FileInputFormat.setInputPaths(job, new Path(input_path));
    Path output=new Path(output_path);
	if(fs.exists(output))
	{
		fs.delete(output, true);
	}
	FileOutputFormat.setOutputPath(job, output);
	return job;
}
	public int run(String[] args) throws Exception
	{
		Configuration conf =getConf();
		Job job =this.createJob(conf, args);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		WCRunner wcRunner =new WCRunner();
		ToolRunner.run( wcRunner,args);
	}

}
