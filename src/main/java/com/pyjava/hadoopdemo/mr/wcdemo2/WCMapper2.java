package com.pyjava.hadoopdemo.mr.wcdemo2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//KEYIN, VALUEIN, KEYOUT, VALUEOUT
//一个文件块，一个map实例，每一行，调用一次 map函数。
// 整个文件块，共享一个实例
public class WCMapper2 extends Mapper<LongWritable,Text,Text,LongWritable> {
	
	private Text keyOut=new Text(); 
	private LongWritable valueOut =new LongWritable();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("map begin......"); //map 任务初始化
		
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("map end......"); //map 任务结束
	}


	@Override
	protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		System.out.println(value);
		System.out.println("---------------------");
		keyOut.set(value.toString());
		valueOut.set(1);
		context.write(keyOut, valueOut);
		System.out.println("---------------------");
	}
	
	

}
