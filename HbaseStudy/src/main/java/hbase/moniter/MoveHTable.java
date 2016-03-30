package hbase.moniter;

import com.hbase.crud.factory.HBaseFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * ClassName: MoveHTable
 * Description: 数据迁移，建表
 * Date: 2016/3/24 10:49
 *
 * @author sm12652
 * @version V1.0
 */
public class MoveHTable {


    /**
     * 表结构迁移
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {

        Configuration conf1 = HBaseConfiguration.create(new Configuration());
        conf1.set("hbase.zookeeper.quorum", "kmaster,kslave01,kslave02");

        Configuration conf2 = HBaseConfiguration.create(new Configuration());
        conf2.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        // 获取同步的集群信息
        HConnection conn = HConnectionManager.createConnection(conf1);
//        HConnection conn2 = HConnectionManager.createConnection(conf2);

        HBaseAdmin hBaseAdmin1 = new HBaseAdmin(conf1);
//        HBaseAdmin hBaseAdmin2 = new HBaseAdmin(conf2);

        HTableDescriptor[] tableDescriptors = hBaseAdmin1.listTables();//获取所有的表
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            TableName tableName = tableDescriptor.getTableName();

            if(Objects.equals("dc_dp_hbm_test_002", tableName.getNameAsString()) || Objects.equals("dc_dp_hbase_test_0001", tableName.getNameAsString())) {
                HTable table = (HTable) conn.getTable(tableName);
                int splitNum = table.getRegionLocations().size();//region数量
                System.out.println(table.getName().getNameAsString() + "  region数量: "+ splitNum);
                byte[][] startKeys = table.getStartKeys();
                byte[][] keys = new byte[splitNum-1][];
                System.arraycopy(startKeys, 1, keys, 0, splitNum-1);

                System.out.println("startKey : " + Bytes.toString(startKeys[1]));
                String endKey = Bytes.toString(startKeys[startKeys.length - 1]);
                System.out.println("endKey : " + endKey);
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("dc_dp_hbase_test_0001_copy"));
                htd.addFamily(new HColumnDescriptor("f"));
                hBaseAdmin1.createTable(htd, keys);

            }
        }


    }
}
