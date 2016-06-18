package com.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: MyFilter
 * Description:
 * Date: 2016/1/25 13:45
 *
 * @author sm12652
 * @version V1.0
 */
public class MyFilter extends FilterBase{

    private byte[] value = null;
    private boolean filterRow = true;

    public MyFilter() {
        super();
    }

    public MyFilter(byte[] value) {
        this.value = value;
    }


    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {

        if(Bytes.compareTo(value, CellUtil.cloneValue(ignored)) == 0) {
            filterRow = false;//每当有值匹配设定的值时，让这一行通过
        }
        return ReturnCode.INCLUDE;//总是包含Cell实例，直到filterRow()决定是否过滤这一行
    }


    @Override
    public boolean filterRow() throws IOException {
        return filterRow;//这是实际上决定数据是否被返回的一行代码，基于标志位判断
    }

    @Override
    public void reset() throws IOException {
        this.filterRow = true;//每当有新行的时候，重置过滤器的值
    }



}

