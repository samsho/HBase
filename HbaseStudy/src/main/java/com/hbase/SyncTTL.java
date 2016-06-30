package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ClassName: SyncTTL
 * Description:
 * Date: 2016/6/1
 * Time: 16:05
 *
 * @author sm12652
 * @version V1.0.6
 */
public class SyncTTL {


    public static void main(String[] args) {
        try {
            System.out.println("++++++++++++++++++++++ 开 *** 始++++++++++++++++++++++");
            SyncTTL.syncTTL();
            System.out.println("++++++++++++++++++++++ 结 *** 束++++++++++++++++++++++");
        } catch (Exception e) {
            System.out.println("++++++++++++++++++++ 错啦 +++++++++++++++++++++");
        }
    }

    public static void syncTTL() throws Exception {
        Configuration conf1 = HBaseConfiguration.create();
//        conf1.set("hbase.zookeeper.quorum","hslave11,hslave12,hslave13,hslave14,hslave15,hslave16,hslave17");
        conf1.set("hbase.zookeeper.property.clientPort", "2181");

        conf1.set("hbase.zookeeper.quorum", "hmaster.localdomain,hslave02.localdomain,hslave01.localdomain");

        Configuration conf2 = HBaseConfiguration.create();
//        conf2.set("hbase.zookeeper.quorum","ZK-186-002,ZK-186-003,ZK-186-004,ZK-186-005,ZK-186-006,ZK-186-007,ZK-186-008");
//        conf2.set("hbase.zookeeper.property.clientPort","2201");

        conf2.set("hbase.zookeeper.quorum", "kmaster,kslave01,kslave02");
        conf2.set("hbase.zookeeper.property.clientPort", "2181");

        HBaseAdmin hBaseAdmin1 = new HBaseAdmin(conf1);
        HBaseAdmin hBaseAdmin2 = new HBaseAdmin(conf2);

        byte[] fimaly = Bytes.toBytes("f");

        HTableDescriptor[] tableDescriptors1 = hBaseAdmin1.listTables();
        for (HTableDescriptor tableDescriptor1 : tableDescriptors1) {

            HColumnDescriptor hColumnDescriptor1 = tableDescriptor1.getFamily(fimaly);
            if (hColumnDescriptor1 != null) {
                int timeToLive = hColumnDescriptor1.getTimeToLive();

                // 非永久的都需要修改
                if (Integer.MAX_VALUE != timeToLive) {
                    TableName tableName = tableDescriptor1.getTableName();
                    HTableDescriptor tableDescriptor2 = hBaseAdmin2.getTableDescriptor(tableName);
                    if (tableDescriptor2 != null) {
                        HColumnDescriptor hColumnDescriptor2 = tableDescriptor2.getFamily(fimaly);
                        if (hColumnDescriptor2 != null) {
                            if (timeToLive != hColumnDescriptor2.getTimeToLive()) {
                                System.out.println("++++++++++++ TableName +++++++++ ： " + tableName + "  ++++++++++++ TTL +++++++++ ： " + timeToLive);
                                hColumnDescriptor2.setTimeToLive(timeToLive);
                                hBaseAdmin2.modifyTable(tableName, tableDescriptor2);
                            }
                        }
                    }
                }
            }
        }

        hBaseAdmin1.close();
        hBaseAdmin2.close();

    }

}
