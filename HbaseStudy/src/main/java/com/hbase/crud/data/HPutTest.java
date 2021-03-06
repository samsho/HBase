package com.hbase.crud.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HPutTest {

    HConnection connection;

    public static void main(String[] args) {

    }

    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
//        conf.set("hbase.zookeeper.quorum","master,slave1,slave3");
        conf.set("hbase.zookeeper.quorum","hmaster.localdomain,hslave02.localdomain,hslave01.localdomain");
        System.out.println(conf.get("hbase.zookeeper.quorum"));
        connection = HConnectionManager.createConnection(conf);
    }

    @Test
    public void testHPut() throws Exception {
        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_test_0001"));
        Put put = new Put(Bytes.toBytes("myRow"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("myCol_2"), Bytes.toBytes("myVal_2"));
        table.put(put);
        table.close();
    }

    @Test
    public void testPut() throws Exception {
        HTableInterface table = connection.getTable(Bytes.toBytes("myTable"));
        for (int i=0; i<100; i++) {
            Put put = new Put(Bytes.toBytes("row"+i));//多行
            put.add(Bytes.toBytes("f"),Bytes.toBytes("col1"),Bytes.toBytes("val"));
            put.add(Bytes.toBytes("f"),Bytes.toBytes("col2"),Bytes.toBytes("val"));
            put.add(Bytes.toBytes("f"),Bytes.toBytes("col3"),Bytes.toBytes("val"));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("col4"), Bytes.toBytes("val"));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("col5"), Bytes.toBytes("val"));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("col6"), Bytes.toBytes("val"));

            table.put(put);
        }

        table.close();

    }


//**********************************************************************************************************************



}
