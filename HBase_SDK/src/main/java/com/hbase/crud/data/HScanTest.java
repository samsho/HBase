package com.hbase.crud.data;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;

public class HScanTest extends TestCase {
    
    public void testSingleHScan() throws Exception{
        long start = System.currentTimeMillis();
        HConnection conn  = HConnectionManager.createConnection(HBaseConfiguration.create(new Configuration()));
        HTableInterface table = conn.getTable(Bytes.toBytes("sdk_test_0002"));
        System.out.println("these Connection total consume ： " + (System.currentTimeMillis()-start));
        Scan scan = new Scan();
        scan.setCaching(6);
//        scan.setBatch(batch);
//        System.out.println(scan.getCaching());
        
        int count = 0;
        ResultScanner resultScanner =  table.getScanner(scan);
        for (Result result : resultScanner) {
            count++;
        }
        System.out.println("these scan total consume ： " + (System.currentTimeMillis() - start));
        resultScanner.close();
        table.close();
        conn.close();
        System.out.println("jishu: " + count);
        long time = System.currentTimeMillis()-start;
        System.out.println("this test total consume ： " + time);
    }
    

    
    
    
    /**
     * 
     * Title: cacheAndBatch 
     * Description: 
     * date: 2015年11月3日 上午10:58:40
     * @param conf
     * @param cache
     * @param batch
     * @throws IOException
     * @author sm12652
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static void cacheAndBatch(Configuration conf, int cache, int batch) throws IOException{
        Logger log = Logger.getLogger("org.apache.hadoop");
        
        final int[] counters = {0,0};
        Appender append = new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent event) {
                String msg = event.getMessage().toString();
                if(msg!=null && msg.contains("Cell： next")) {
                    counters[0]++;
                }
            }
            @Override
            public boolean requiresLayout() {
                return false;
            }
            
            @Override
            public void close() {
                
            }
        };
        log.removeAllAppenders();
        log.setAdditivity(false);
        log.addAppender(append);
        log.setLevel(Level.DEBUG);
        
        HConnection conn = HConnectionManager.createConnection(conf);
        HTableInterface table = conn.getTable("sdk_conn_test");
        
        Scan scan = new Scan();
        scan.setCaching(cache);
        scan.setBatch(batch);
        
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            counters[1]++;
        }
        
        resultScanner.close();
        System.out.println(
                "Cache : "+ cache 
                +",Batch : " + batch 
                + ", Results :  "+ counters[1]
                + ", RPCs : "+  counters[0]
        );
        
        
        
        
    }
    
    
    
}
