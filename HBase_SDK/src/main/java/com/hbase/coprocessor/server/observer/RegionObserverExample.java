package com.hbase.coprocessor.server.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.List;

/**
 * ClassName: RegionObserverExample
 * Description:
 * Date: 2016/1/21 16:20
 *
 * @author sm12652
 * @version V1.0
 */
public class RegionObserverExample extends BaseRegionObserver{

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        System.out.println("..........get运行之前打印出来........");
        e.bypass();
    }
}
