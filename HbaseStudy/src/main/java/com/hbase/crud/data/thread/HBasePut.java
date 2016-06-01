package com.hbase.crud.data.thread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBasePut {
    private transient volatile static Logger logger = LoggerFactory.getLogger(HBasePut.class);

    static HConnection connection;
    static{
            Configuration conf = HBaseConfiguration.create(new Configuration());
        System.out.println(conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    try {
        connection = HConnectionManager.createConnection(conf);
    } catch (IOException e) {
        e.printStackTrace();
    }

}


    public static void MultThreadInsert(int threadNumber, int count, int num) throws InterruptedException {
        logger.info("---------start threadInsert test----------");
        long start = System.currentTimeMillis();
//        int threadNumber = 10;
        Thread[] threads=new Thread[threadNumber];
        for(int i=0;i<threads.length;i++) {
            threads[i]= new ImportThread(count,num);
            threads[i].start();
        }

        System.out.println("--------MultThreadInsert----------");
        logger.info("---------end threadInsert test----------");
        long time = System.currentTimeMillis() - start;
        logger.info("++++++++++++++++++++++++++++ TOTAL PUTS NEED  +++++++++++++++++++++   " + time + " ms");
    }

    public static class ImportThread extends Thread{
        int count;
        int num;
        ImportThread (int count,int num){
            this.count = count;
            this.num = num;
        }
        public void run(){
            try{
                InsertProcess(count,num);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void InsertProcess(int count,int num) throws Exception {
        logger.info(Thread.currentThread().getName() + ".........................start....................");
        long start = System.currentTimeMillis();
//        Configuration conf = HBaseConfiguration.create(new Configuration());
//        HConnection connection = HConnectionManager.createConnection(conf);

        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_task_128"));
        System.out.println(table);
        table.setAutoFlushTo(false);
        table.setWriteBufferSize(24 * 1024 * 1024);//24M

        List<Put> list = new ArrayList<Put>();
//        int count = 10000;
        byte[] row;
        for(int i=0;i<count;i++)  {
            row = Bytes.toBytes(MD5Hash.getMD5AsHex((i + Thread.currentThread().getName()).getBytes()));
            Put put = new Put(row);
            put.add("f".getBytes(), ("col" + i).getBytes(), ("val" + i).getBytes());
            list.add(put);
            if(list.size()==num){
                table.put(list);
                list.clear();
                table.flushCommits();
                logger.info(Thread.currentThread().getName() + ".........................putting....................");
            }
        }
        if(!list.isEmpty()){
            table.put(list);
            list.clear();
            table.flushCommits();
        }

        table.close();
        long time = System.currentTimeMillis() - start;
        logger.info(
                Thread.currentThread().getName() + ".........................end....................and need  " + time
                        + " ms");

    }


    public static void main(String[] args)  throws Exception{
//        MultThreadInsert(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        MultThreadInsert(400, 15000, 5000);

//        System.out.println(DateUtil.parseDate("2015-12-01").getTime());

    }

}