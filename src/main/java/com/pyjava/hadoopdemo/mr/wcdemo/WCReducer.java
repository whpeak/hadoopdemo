package com.pyjava.hadoopdemo.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, Text>{
	private Text keyOut=new Text(); 
	private Text valueOut=new Text();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		keyOut.set(key);
		long sum=0l;
		for(LongWritable count :values)
		{
			sum+=count.get();	
		}
		valueOut.set(sum+"");
		context.write(keyOut, valueOut);
	}
	

}
