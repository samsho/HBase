package com.hbase.coprocessor.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

public class MyAggregationClient {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MyAggregationClient.class);

    private static final byte[] TABLE_NAME = Bytes.toBytes("myTable");
    private static final byte[] CF = Bytes.toBytes("f");

    public static void main(String[] args) throws Throwable {
        aggregationClient();
    }

    /**
     * 获得符合条件结果总数
     * @author
     * @param
     * @return
     */

    public static void aggregationClient() {
        Configuration conf = HBaseConfiguration.create(new Configuration());
//        conf.set("hbase.zookeeper.quorum","kmaster,kslave01,kslave02");
        AggregationClient aggregationClient = new AggregationClient(conf);

        try {
            Scan scan ;
            scan= new Scan();
            scan.addFamily(Bytes.toBytes("f"));//必须有此句，或者用addFamily(),否则出错，异常包含 ci ****
            scan.setStartRow(Bytes.toBytes("row1"));
            scan.setStopRow(Bytes.toBytes("row10"));


            LongColumnInterpreter longColumnInterpreter = new LongColumnInterpreter();
            long rowCount  = aggregationClient.rowCount(TableName.valueOf(TABLE_NAME), longColumnInterpreter, scan);
            System.out.println("+++++++++++++++++   rowCount   +++++++++++++++++++++++       " + rowCount);
        } catch (Throwable e) {
            logger.error("getTotalNumber wrong. ", e);
        }

    }
/*
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
    }*/

}