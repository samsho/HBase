package com.hbase.coprocessor.client;

import com.hbase.coprocessor.server.observer.RegionObserverExample;
import com.hbase.hdfs.HDFSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * ClassName: RegionObserverTest
 * Description:
 * Date: 2016/1/21 16:24
 *
 * @author sm12652
 * @version V1.0
 */
public class RegionObserverTest {

    Configuration conf;
    HConnection conn;
    HBaseAdmin admin;
    Path path;

    @Before
    public void before() throws Exception {
        conf = HBaseConfiguration.create(new Configuration());
        conn = HConnectionManager.createConnection(conf);
        admin = new HBaseAdmin(conf);
        path = new Path("hdfs://master:9000/hbasesdk/coprocessor/coprocessor.jar");
    }

    @Test
    public void copyFromLocal() throws Exception {
        HDFSClient.copyFromLocal(conf,"D:\\ProjectSpace\\Idea\\SAM-SHO\\HBase\\HBase_SDK\\target\\HBase_SDK.jar","hdfs://master:9000/hbasesdk/coprocessor/coprocessor.jar");
    }

    @Test
    public void createTable() throws IOException {
        TableName tableName = TableName.valueOf("regionObserver_test");
        HTableDescriptor htd = new HTableDescriptor(tableName);

        htd.addCoprocessor(RegionObserverExample.class.getCanonicalName(), path,
                Coprocessor.PRIORITY_USER, null);

        HColumnDescriptor hd = new HColumnDescriptor("f");
        htd.addFamily(hd);
        admin.createTable(htd);
    }

    @Test
    public void modifyTable() throws IOException {
        TableName tableName = TableName.valueOf("regionObserver_test");

        admin.disableTable(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnFamily1 = new HColumnDescriptor("f");
        columnFamily1.setMaxVersions(2);
        hTableDescriptor.addFamily(columnFamily1);


        hTableDescriptor.addCoprocessor(RegionObserverExample.class.getCanonicalName(), path,
                Coprocessor.PRIORITY_SYSTEM, null);
        admin.modifyTable(tableName, hTableDescriptor);
        admin.enableTable(tableName);
    }

    @Test
    public void preGet() throws IOException {
        TableName tableName = TableName.valueOf("regionObserver_test");
        byte[] row = Bytes.toBytes("row");
        Get get = new Get(row);
        HTableInterface table = conn.getTable(tableName);
        Result result =  table.get(get);
        for(Cell cell :result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }


}
