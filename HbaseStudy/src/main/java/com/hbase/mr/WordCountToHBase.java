package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * ClassName: WordCountToHBase
 * Description:
 * Date: 2016/6/25 16:38
 *
 * @author SAM SHO
 * @version V1.0
 */
public class WordCountToHBase extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new WordCountToHBase(), args);
    }

    @Override
    public int run(String[] args) throws Exception {


        return 0;
    }

    /**
     *
     */
    class TableMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();




        }
    }


}
