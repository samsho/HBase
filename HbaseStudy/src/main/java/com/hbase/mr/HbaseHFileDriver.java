/*
package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HbaseHFileDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "testhbasehfile");
        job.setJarByClass(HbaseHFileDriver.class);

        job.setMapperClass(HBaseHFileMapper.class);
        job.setReducerClass(com.test.hfile.HBaseHFileReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 偷懒， 直接写死在程序里了，实际应用中不能这样, 应从命令行获取
        FileInputFormat.addInputPath(job, new Path("/home/yinjie/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/yinjie/output"));

        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "localhost");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseConfiguration cfg = new HBaseConfiguration(HBASE_CONFIG);
        String tableName = "t1";
        HTable htable = new HTable(cfg, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, htable);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


} */
