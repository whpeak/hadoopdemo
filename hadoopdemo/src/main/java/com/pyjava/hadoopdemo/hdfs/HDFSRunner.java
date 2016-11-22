package com.pyjava.hadoopdemo.hdfs;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.pyjava.hadoopdemo.common.hadoop.ConfCommon;
import com.pyjava.hadoopdemo.common.hadoop.FSCommon;
import com.pyjava.hadoopdemo.hdfs.util.FileUtils;

/**
 * 
 * @author wangheng
 *hdfs、mr都可以使用该方式执行。获取配置文件的重点，就是继承 Configured 类
 *如果是调用远程的hdfs（不是在集群中的一个节点上运行），就需要指定core和hdfs的配置文件。
 *如果是在本地执行，就不用指定配置文件。
 *继承Configured 类，就可以获取默认的配置文件
 *具体步骤：
 *step1:获取默认的配置文件
 *step2:更新自定义的配置文件
 *step3:根据配置文件，获取文件系统
 *
 */
public class HDFSRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		conf = ConfCommon.updateConf(conf);
		FileSystem fs = FSCommon.getFS(conf);
		String path="/Users/wangheng/test";
		List<String> listFiles = FileUtils.listFiles(fs, path);
		System.out.println(listFiles);
		System.out.println(fs.getUri().toString());
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		HDFSRunner tool =new HDFSRunner();
		ToolRunner.run(tool, args);
	}

}
