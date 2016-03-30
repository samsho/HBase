package com.hbase.crud.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseGet {
    private transient volatile static Logger logger = LoggerFactory.getLogger(HBaseGet.class);
    static HConnection connection;
    static{
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            connection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args)  throws Exception{
//        threadInsert(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        threadInsert(200,10000);
    }

    public static void threadInsert(int threadNumber, int count) throws InterruptedException {
        logger.info("---------start threadInsert test----------");
        long start = System.currentTimeMillis();
        Thread[] threads=new Thread[threadNumber];
        for(int i=0;i<threads.length;i++) {
            threads[i]= new ImportThread(count);
            threads[i].start();
        }

        logger.info("---------end threadInsert test----------");
        long time = System.currentTimeMillis() - start;
        logger.info("++++++++++++++++++++++++++++ TOTAL GETS NEED  +++++++++++++++++++++   " + time + " ms");
    }

    public static class ImportThread extends Thread{
        int count;
        ImportThread (int count){
            this.count = count;
        }
        public void run(){
            try{
                InsertProcess(count);
            }catch(Exception e){
                e.printStackTrace();
            }finally {
                System.gc();
            }
        }
    }

    public static void InsertProcess(int count) throws Exception {
        logger.info(Thread.currentThread().getName() + ".........................start....................");
        long start = System.currentTimeMillis();

        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_test_0001"));
        System.out.println(table);

        List<Get> gets = new ArrayList<Get>();
//        int count = 100000;
        for(int i=0;i<count;i++)  {
            Get get = new Get(Bytes.toBytes(MD5Hash.getMD5AsHex((i + "row").getBytes())));
            get.addColumn("f".getBytes(), ("col" + i).getBytes());
            gets.add(get);
        }

        Result[] rels = table.get(gets);
        table.close();
        long time = System.currentTimeMillis() - start;
        logger.info(Thread.currentThread().getName() + "++++++++++++++++++++++++++++++++++end++++++++++++++++++++++++++++++++++and need  "+time +" ms");


    }


}