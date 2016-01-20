package com.hbase.coprocessor.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;

public class MyAggregationClient {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MyAggregationClient.class);

    private static final byte[] TABLE_NAME = Bytes.toBytes("myTable");
    private static final byte[] CF = Bytes.toBytes("f");

    public static void main(String[] args) throws Throwable {
        Configuration configuration = HBaseConfiguration.create(new Configuration());
        HTableInterface tablle = HConnectionManager.createConnection(configuration).getTable(TABLE_NAME);
        doCoprocessor(tablle);

    }

    /**
     * 获得符合条件结果总数
     * @author wanglongyf2 2013-1-11 上午10:29:15
     * @param scan
     * @return
     */

    public static long getTotalNumber(Scan scan) {
        Configuration configuration = HBaseConfiguration.create(new Configuration());
        AggregationClient aggregationClient = new AggregationClient(configuration);
        long rowCount = 0;
        try {
            scan.addFamily(Bytes.toBytes("f"));//必须有此句，或者用addFamily(),否则出错，异常包含 ci ****
            rowCount = aggregationClient.rowCount(TableName.valueOf(TABLE_NAME), null, scan);
        } catch (Throwable e) {
            logger.error("getTotalNumber wrong. ", e);
        }
        return rowCount;
    }

    private static void doCoprocessor(HTableInterface table) throws IOException {
        long numberOfResults = 0;
        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("col"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("row"));
        scan.setFilter(filter);
        long number = getTotalNumber(scan);
        ResultScanner scanner = table.getScanner(scan);
        Result res = scanner.next();
        while(res != null) {
            numberOfResults ++;
            res = scanner.next();
        }
        if (numberOfResults != number) {
            logger.error(String.format("use aggregation %d and scanner %d gets inconsistant result. ",
                    number, numberOfResults));
        }
    }

}