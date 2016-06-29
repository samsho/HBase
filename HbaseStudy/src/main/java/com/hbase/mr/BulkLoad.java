package com.hbase.mr;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.sina.hbase.connection.ConnectionPool;
import com.sina.hbase.utils.DataOptUtil;
import com.sina.hbase.utils.Util;


public class BulkLoad {

	public static class myMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 检查并初始化数据对象
			String p = value.toString();

			if (p != null) {
				byte[] row = Bytes.toBytes(p.getUid());
				ImmutableBytesWritable k = new ImmutableBytesWritable(row);
				KeyValue kv = new KeyValue(row, "c".getBytes(), "c".getBytes(),
						p.toByteArray());
				context.write(k, kv);

			}
		}
	}

	

	/**
	 * 通过表名决定使用哪种Mapper，如果表名不存在则返回null
	 * 
	 * @param tableName
	 * @return
	 */

	@SuppressWarnings("rawtypes")
	public static Class<? extends Mapper> decideMapper(String tableName) {
		if (tableName.equals("***"))
			return myMapper.class;
		

		return null;
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err
					.println("Usage: BulkLoad <inputPath> <hfilePath> <tablename>");
			System.exit(2);
		}
		Configuration conf = HBaseConfiguration.create();

		HTable table  = (HTable) HConnectionManager.createConnection(conf).getTable(args[2]);

		Job job = Job.getInstance(conf, "BulkLoad-" + args[2] + "-");

		// 根据表的不同选择mapper

		job.setMapperClass(decideMapper(args[2]));

		job.setJarByClass(BulkLoad.class);
		job.setInputFormatClass(LzoTextInputFormat.class);//添加压缩

		HFileOutputFormat2.configureIncrementalLoad(job, table);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

