package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 写入HBase
 */
public class TxtHbase {
    public static void main(String[] args) throws Exception {
        int mr = ToolRunner.run(new Configuration(), new THDriver(), args);
        System.exit(mr);
    }


    public static class THDriver extends Configured implements Tool {

        @Override
        public int run(String[] arg0) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum.", "bfdbjc2:2181,bfdbjc3:2181,bfdbjc4:2181");  //千万别忘记配置

            Job job = Job.getInstance(conf, "Txt-to-Hbase");
            job.setJarByClass(TxtHbase.class);

            Path in = new Path("hdfs://bfdbjc1:12000/user/work/a.txt");

            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, in);

            job.setMapperClass(THMapper.class);
            job.setReducerClass(THReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            TableMapReduceUtil.initTableReducerJob("tab1", THReducer.class, job);

            job.waitForCompletion(true);
            return 0;
        }

    }

    public class THMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) {
            String[] items = value.toString().split(" ");
            String k = items[0];
            String v = items[1];
            System.out.println("key:" + k + "," + "value:" + v);
            try {
                context.write(new Text(k), new Text(v));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    public class THReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) {
            String k = key.toString();
            String v = value.iterator().next().toString(); //由数据知道value就只有一行
            Put putrow = new Put(k.getBytes());
            putrow.add("f1".getBytes(), "qualifier".getBytes(), v.getBytes());
            try {

                context.write(new ImmutableBytesWritable(key.getBytes()), putrow);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}