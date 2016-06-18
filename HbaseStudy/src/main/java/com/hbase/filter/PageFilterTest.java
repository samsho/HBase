package com.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
        conf.set("hbase.zookeeper.quorum", "hmaster.localdomain,hslave02.localdomain,hslave01.localdomain");
        conn = HConnectionManager.createConnection(conf);
    }


    @Test
    public void save() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_task_1");
        Put put = new Put(Bytes.toBytes("r"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("v"));

        table.put(put);
        table.close();
    }

    @Test
    public void get() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_task_1");
        Get get = new Get(Bytes.toBytes("r"));
        get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            prt(cell);
        }

        table.close();
    }

    @Test
    public void scan() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_task_1");
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


    /**
     * PageFilter 测试
     * 每个 RS 的每个 region 都会取 PageSize 数据
     * @throws IOException
     */
    @Test
    public void TestPageFilter() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_task_12");
        Scan scan = new Scan();
        scan.setCacheBlocks(false);

        PageFilter pageFilter = new PageFilter(2);//行分页过滤器，每页返回10行
        scan.setFilter(pageFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                prt(cell);
            }
        }
    }


    /**
     *
     * @throws IOException
     */
    @Test
    public void testPageFilter() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_task_12");
        Filter filter = new PageFilter(2);

        final byte[] POSTFIX = new byte[]{0x00};
        byte[] lastRow = null;
        int totalRows = 0;

        while (true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                //注意这里添加了POSTFIX操作，不然死循环了
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                scan.setStartRow(startRow);//[},包前不包后
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
                System.out.println(localRows++ + ":" + result);
                totalRows++;
                lastRow = result.getRow();
                /*if (totalRows == 5) {
                    localRows = 0;
                    break;
                }*/
            }
            scanner.close();
            if (localRows == 0 ) break;

        }
        System.out.println("total rows:" + totalRows);
    }

    /**
     * 行列简单使用
     * @throws IOException
     */
    @Test
    public void scanWithFilter() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_log_analysis");

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(6);
//        scan.setBatch(2);//不能和filter一起使用，看源码

        FilterList filterList = new FilterList();
        PageFilter pageFilter = new PageFilter(1);//行分页过滤器，每页返回10行
        filterList.addFilter(pageFilter);
        // 列分页(>=offset,并且列数<=limit)
        // 从第3列开始,取2列，计数从0开始
        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(4, 0);
        filterList.addFilter(columnPaginationFilter);

        scan.setFilter(filterList);

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

    /**
     * 分页取数据
     * @throws IOException
     */
    @Test
    public void testColumnFilter() throws IOException {
        HTableInterface table = conn.getTable("dc_dp_log_analysis");
        PageFilter pageFilter = new PageFilter(1);
        final byte[] POSTFIX = new byte[]{0x00};
        byte[] lastRow = null;


        while (true) {
            int index = 0;
            int count = 10;
            Scan scan = new Scan();
            if (lastRow != null) {
                //注意这里添加了POSTFIX操作，不然死循环了
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                scan.setStartRow(startRow);//[},包前不包后
            }

            scan.setCacheBlocks(false);
            scan.setCaching(6);
            int localRow = 0;

            while (true) {
                ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(count, index);//每行取4列
                FilterList filterList = new FilterList();
                filterList.addFilter(pageFilter);
                filterList.addFilter(columnPaginationFilter);
                scan.setFilter(filterList);
                ResultScanner resultScanner = table.getScanner(scan);
                int localCol = 0;
                for (Result result : resultScanner) {
                    localRow++;
                    localCol++;

                    System.out.println("+localRow+" + localRow + " ----- " + "+localCol+" + localCol);
                    for (Cell cell : result.rawCells()) {
                        prt(cell);
                    }
                    lastRow = result.getRow();
                }

                index += count;
                resultScanner.close();
                if (localCol == 0) break;
            }

            if (localRow == 0) break;
        }

        table.close();
        conn.close();
    }

    private FilterList addFilters() {
        FilterList filterList = new FilterList();
//        PageFilter pageFilter = new PageFilter(1);//行分页过滤器，每页返回10行
//        filterList.addFilter(pageFilter);
        // 列分页(>=offset,并且列数<=limit)
        // 从第3列开始,取2列，计数从0开始
        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(4, 0);
        filterList.addFilter(columnPaginationFilter);

        return filterList;
    }

    private void prt(Cell cell) {
        System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + " + " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + " + " + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + " + " + Bytes.toString(CellUtil.cloneValue(cell))
        );
    }

    @Test
    public void testPageF() throws IOException {
        String startRow = testPage("dc_dp_task_1", 1, null, 5, 0);
        System.out.println("startRow : ~~ " + startRow);
        testPage("dc_dp_task_1", 1, startRow, 5, 0);
//          testPage("regionObserver_test", 5, testPage("regionObserver_test", 5, null));
    }

    @Deprecated
    public String testPage(String tableName, long pageSize, String startRow, int count, int index) throws IOException {
        String lastRow = null;
        final byte[] POSTFIX = new byte[]{0x00};

        HTableInterface table = conn.getTable(tableName);
        PageFilter pageFilter = new PageFilter(pageSize);
        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(count, index);

        FilterList filterList = new FilterList();
        filterList.addFilter(pageFilter);
        filterList.addFilter(columnPaginationFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        if (startRow != null) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            //装数据如返回
            for (Cell cell : result.rawCells()) {
                prt(cell);
            }
            //TODO 错了
            if (result.isEmpty()) {
                lastRow = Bytes.toString(Bytes.add(result.getRow(), POSTFIX));
                System.out.println("lastRow : ~~ " + lastRow);
            }
        }
        return lastRow;
    }

}
