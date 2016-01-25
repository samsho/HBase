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
    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        connection = HConnectionManager.createConnection(conf);
    }

    @Test
    public void testHPut() throws Exception {
        HTableInterface table = connection.getTable(Bytes.toBytes("table_desc_0001"));
        Put put = new Put(Bytes.toBytes("myRow"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("myCol_1"), Bytes.toBytes("myVal_1"));
        table.put(put);
        table.close();
    }

    @Test
    public void testPut() throws Exception {
        HTableInterface table = connection.getTable(Bytes.toBytes("regionObserver_test"));
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
