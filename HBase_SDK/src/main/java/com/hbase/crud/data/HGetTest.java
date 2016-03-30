package com.hbase.crud.data;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HGetTest {

    HConnection connection;
    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        connection = HConnectionManager.createConnection(conf);
    }

    @Test
    public void testHGet() throws Exception{
        long start = System.currentTimeMillis();
        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_test_0001"));
        Get get = new Get(Bytes.toBytes("myRow"));
        Result result = table.get(get);
        System.out.println(result);
        table.close();
        long time = System.currentTimeMillis()-start;
        
        System.out.println("this get total consume ： " + time);

        Thread.sleep(20000);
        Thread.sleep(10000);

    }


    @Test
    public void testGets() throws Exception{
        long start = System.currentTimeMillis();
        HConnection conn  = HConnectionManager.createConnection(HBaseConfiguration.create(new Configuration()));
        HTableInterface table = conn.getTable(Bytes.toBytes("sdk_test_0002"));
        System.out.println("these Connection total consume ： " + (System.currentTimeMillis()-start));
        List<Get> gets = Lists.newArrayList();
        for (int i = 0; i < 2000000; i++) {
            Get get = new Get(Bytes.toBytes(MD5Hash.getMD5AsHex((i + "row").getBytes())));
            gets.add(get);
        }
        table.get(gets);
        System.out.println("these get total consume ： " + (System.currentTimeMillis() - start));
        table.close();
        conn.close();
        long time = System.currentTimeMillis()-start;
        System.out.println("these test total consume ： " + time); // 58941
    }
}
