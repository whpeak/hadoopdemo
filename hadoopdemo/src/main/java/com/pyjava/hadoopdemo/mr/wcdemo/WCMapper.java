package com.pyjava.hadoopdemo.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//KEYIN, VALUEIN, KEYOUT, VALUEOUT
//一个文件块，一个map实例，每一行，调用一次 map函数。
// 整个文件块，共享一个实例
public class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
	private Text keyOut=new Text(); 
	private LongWritable valueOut =new LongWritable();

	@Override
	protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		System.out.println(value);
		keyOut.set(value.toString());
		valueOut.set(1);
		context.write(keyOut, valueOut);
	}
	
	

}
