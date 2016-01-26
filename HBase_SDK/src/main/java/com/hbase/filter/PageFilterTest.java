package com.hbase.filter;

import com.hbase.crud.factory.HBaseFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * ClassName: PageFilterTest
 * Description:
 * Date: 2016/1/25 14:13
 *
 * @author sm12652
 * @version V1.0
 */
public class PageFilterTest {
    HConnection conn;
    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.quorum","kmaster,kslave01,kslave02");
        conn = HConnectionManager.createConnection(conf);
    }

    @Test
    public void scan() throws IOException {
        HTableInterface table = conn.getTable("regionObserver_test");
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                prt(cell);
            }
        }

        resultScanner.close();
        table.close();
        conn.close();
    }


    @Test
    public void scanWithFilter() throws IOException {
        HTableInterface table = conn.getTable("regionObserver_test");

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(6);
//        scan.setBatch(2);//不能和filter一起使用，看源码

        FilterList filters = addFilters();
        scan.setFilter(filters);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                prt(cell);
            }
        }

        resultScanner.close();
        table.close();
        conn.close();
    }

    private FilterList addFilters() {
        FilterList filterList = new FilterList();
        PageFilter pageFilter = new PageFilter(10);//行分页过滤器，每页返回10行
        filterList.addFilter(pageFilter);
        // 列分页(>=offset,并且列数<=limit)
        // 从第3列开始,取2列，计数从0开始
        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(2, 0);
        filterList.addFilter(columnPaginationFilter);

        return filterList;
    }

    private void prt(Cell cell) {
        System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "-" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "-" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "-" + Bytes.toString(CellUtil.cloneValue(cell))
        );
    }



    @Test
    public void testPageFilter() throws IOException {
        HTableInterface table = conn.getTable("regionObserver_test");
        Filter filter = new PageFilter(15);

        final byte[] POSTFIX = new byte[] { 0x00 };
        byte[] lastRow = null;
        int totalRows = 0;

        while (true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if(lastRow != null){
                //注意这里添加了POSTFIX操作，不然死循环了
                byte[] startRow = Bytes.add(lastRow,POSTFIX);
                scan.setStartRow(startRow);//[},包前不包后
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while((result = scanner.next()) != null){
                System.out.println(localRows++ + ":" + result);
                totalRows ++;
                lastRow = result.getRow();
            }
            scanner.close();
            if(localRows == 0) break;
        }
        System.out.println("total rows:" + totalRows);
    }



    @Test
    public void testPageF() throws IOException {
        String startRow = testPage("regionObserver_test", 5, null);
        System.out.println("startRow : ~~ " + startRow);
        testPage("regionObserver_test", 5, startRow);
//          testPage("regionObserver_test", 5, testPage("regionObserver_test", 5, null));
    }

    public  String testPage(String tableName, long pageSize, String startRow) throws IOException {
        String lastRow = null;
        int localRows = 0;
        final byte[] POSTFIX = new byte[] { 0x00 };

        HTableInterface table = conn.getTable(tableName);
        Filter filter = new PageFilter(pageSize);

        Scan scan = new Scan();
        scan.setFilter(filter);
        if(startRow!=null) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            //装数据如返回
            System.out.println(result);
            lastRow = Bytes.toString(Bytes.add(result.getRow(), POSTFIX));
        }
        return lastRow;
    }
}
