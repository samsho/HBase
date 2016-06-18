package com.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: SkipFilterTest
 * Description:
 * Date: 2016/4/5 16:56
 *
 * @author sm12652
 * @version V1.0
 */
public class SkipFilterTest {

    public static void main(String[] args) throws Exception {
//        SkipFilterTest.test();
        SkipFilterTest.test2();
    }

    public static void test2() throws Exception {

        Configuration conf = HBaseConfiguration.create(new Configuration());
        HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_task_1"));//表 dc_dp_task_1


        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("f"),Bytes.toBytes("1_1"),
                CompareFilter.CompareOp.EQUAL, new SubstringComparator("1_1"));
        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))
                                +
                                Bytes.toString(CellUtil.cloneQualifier(cell))
                                +
                                Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
    }



    public static void test() throws Exception {

        Configuration conf = HBaseConfiguration.create(new Configuration());
        HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable(Bytes.toBytes("dc_dp_task_1"));

        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1_1")));
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1_1")));
        FilterList filterList = new FilterList();
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(valueFilter);
        SkipFilter skipFilter = new SkipFilter(filterList);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("1_1"));
        scan.setCacheBlocks(false);
        scan.setFilter(filterList);
        scan.setFilter(skipFilter);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))
                        +
                        Bytes.toString(CellUtil.cloneQualifier(cell))
                        +
                        Bytes.toString(CellUtil.cloneValue(cell))
                          );
            }
        }
    }
}
