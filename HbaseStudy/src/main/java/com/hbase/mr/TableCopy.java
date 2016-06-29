package com.hbase.mr;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 *  1、使用TableMapper来读取表
 	2、写入表的第一种方式是用TableMapReduceUtil.initTableReducerJob的方法，
 	这里既可以在map阶段输出，也能在reduce阶段输出。
 	区别是Reduce的class设置为null或者实际的reduce 以下是一个表copy的例子：
 */
public class TableCopy extends Configured implements Tool{
	
	static class CopyMapper extends TableMapper<ImmutableBytesWritable,Put>{

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//将查询结果保存到list
			List<KeyValue> kvs =  value.list();
			Put p = new Put(Bytes.toBytes(""));
			//将结果装载到Put
			for(KeyValue kv : kvs)
				p.add(kv);
			//将结果写入到Reduce
			context.write(key, p);
		}
		
	}
	
	public static Job createSubmittableJob(Configuration conf, String[] args)throws IOException{
		String jobName = args[0];
		String srcTable = args[1];
		String dstTable = args[2];
		Scan sc = new Scan();
		sc.setCaching(10000);
		sc.setCacheBlocks(false);
		Job job = Job.getInstance(conf,jobName);
		job.setJarByClass(TableCopy.class);
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob(srcTable, sc, CopyMapper.class, ImmutableBytesWritable.class, Result.class, job);
		TableMapReduceUtil.initTableReducerJob(dstTable, null, job);
		return job;
		
	}
	
	@Override
	public int run(String[] args)throws Exception{
		Job job = createSubmittableJob(getConf(), args);
		return job.waitForCompletion(true)? 0 : 1;
	}
	
}