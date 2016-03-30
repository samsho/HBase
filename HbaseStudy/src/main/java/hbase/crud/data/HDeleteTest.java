package hbase.crud.data;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HDeleteTest {

    HConnection connection;
    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        connection = HConnectionManager.createConnection(conf);
    }

    
    /**单行单列**/
    @Test
    public void testHDelete() throws Exception{
       
        long start = System.currentTimeMillis();
        HTableInterface table = connection.getTable(Bytes.toBytes("sdk_test_0002"));
        Delete delete = new Delete(Bytes.toBytes("myRow"));
//        delete.deleteColumn("f","col");
//        delete.deleteFamily("f");
        table.delete(delete);
        table.close();

        long time = System.currentTimeMillis()-start;
        System.out.println("this testHDelete total consume ： " + time);//1422

        Thread.sleep(10000);

    }

    /**单行多列**/
    public void testListHDelete1() throws Exception{

        long start = System.currentTimeMillis();
        HTableInterface table = connection.getTable(Bytes.toBytes("sdk_test_0002"));
        Delete delete = new Delete(Bytes.toBytes("myRow"));
        for (int i = 0; i < 5; i++) {
//            delete.deleteColumn("f", "col" + i);
        }
        table.delete(delete);
        table.close();

        long time = System.currentTimeMillis()-start;
        System.out.println("this testListHDelete total consume ： " + time);//29162

        Thread.sleep(10000);
    }

    /**多行单列删除**/
    /**
     *
     * @throws Exception
     */
    public void testListHDelete() throws Exception{
        
        long start = System.currentTimeMillis();
        HTableInterface table = connection.getTable(Bytes.toBytes("sdk_test_0002"));
        System.out.println("these Connection total consume ： " + (System.currentTimeMillis()-start)); //1805 | 1814 | 1815
        List<Delete> deletes = Lists.newArrayList();
        for (int i = 0; i < 200; i++) {
            Delete delete = new Delete(Bytes.toBytes("myRow"+i));
//            delete.deleteColumn("f","col");
//            delete.deleteColumn("f", "col01");
//            delete.deleteColumn("f", "col02");
//            delete.deleteColumn("f", "col03");
            deletes.add(delete);
        }
        table.delete(deletes);
        System.out.println("these Connection total consume ： " + (System.currentTimeMillis() - start));
        table.close();
        long time = System.currentTimeMillis()-start;
        System.out.println("this testListHDelete total consume ： " + time);//130690|152156

        Thread.sleep(10000);
    }


    
}
