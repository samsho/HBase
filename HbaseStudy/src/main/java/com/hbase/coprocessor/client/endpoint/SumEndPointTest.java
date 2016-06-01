package com.hbase.coprocessor.client.endpoint;

import com.google.protobuf.ServiceException;
import com.hbase.coprocessor.proto.SumEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;
import java.util.Map;

public class SumEndPointTest {

public void test() throws IOException {


    Configuration conf = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("users");

    final SumEndpoint.SumRequest request = SumEndpoint.SumRequest.newBuilder().setFamily("salaryDet").setColumn("gross")
            .build();
    try {
        Map<byte[], Long> results = table.coprocessorService (SumEndpoint.SumService.class, null, null,
                new Batch.Call<SumEndpoint.SumService, Long>() {
                    @Override
                    public Long call(SumEndpoint.SumService aggregate) throws IOException {
                        BlockingRpcCallback rpcCallback = new BlockingRpcCallback();
                        aggregate.getSum(null, request, rpcCallback);
                        SumEndpoint.SumResponse response = (SumEndpoint.SumResponse) rpcCallback.get();
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
