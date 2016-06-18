package com.hbase.filter;

import org.apache.hadoop.hbase.filter.PageFilter;

/**
 * ClassName: PageFilterImpl
 * Description:
 * Date: 2016/5/25
 * Time: 16:58
 *
 * @author sm12652
 * @version V1.0.6
 */
public class PageFilterImpl extends PageFilter {
    /**
     * Constructor that takes a maximum page size.
     *
     * @param pageSize Maximum result size.
     */
    public PageFilterImpl(long pageSize) {
        super(pageSize);
    }






}
