/*
package com.hbase.coprocessor.client.endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;

*/
/**
 * Created by home on 2016/1/24.
 *//*

public class SumEndPointTest {

public void test() throws IOException {


//    Configuration conf = HBaseConfiguration.create();
//    // Use below code for HBase version 1.x.x or above.
//    Connection connection = ConnectionFactory.createConnection(conf);
//    TableName tableName = TableName.valueOf("users");
//    Table table = connection.getTable(tableName);


    //Use below code HBase version 0.98.xx or below.
    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("users");

    final SumRequest request = SumRequest.newBuilder().setFamily("salaryDet").setColumn("gross")
            .build();
    try {
        Map<byte[], Long> results = table.CoprocessorService (SumService.class, null, null,
                new Batch.Call<SumService, Long>() {
                    @Override
                    public Long call(SumService aggregate) throws IOException {
                        BlockingRpcCallback rpcCallback = new BlockingRpcCallback();
                        aggregate.getSum(null, request, rpcCallback);
                        SumResponse response = rpcCallback.get();
                        return response.hasSum() ? response.getSum() : 0L;
                    }
                });
        for (Long sum : results.values()) {
            System.out.println("Sum = " + sum);
        }
    } catch (ServiceException e) {
        e.printStackTrace();
    } catch (Throwable e) {
        e.printStackTrace();
    }
}

}
*/
