package com.hbase.tableadmin.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * ClassName: HBaseAdminDemo
 * Description:
 * Date: 2015/12/3 17:16
 *
 * @author sm12652
 * @version V1.0
 */
public class HBaseAdminDemo {

    @Test
    public void createTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("cluster_table"));

        HColumnDescriptor hd = new HColumnDescriptor("f");
        htd.addFamily(hd);
        hBaseAdmin.createTable(htd);
        hBaseAdmin.flush("cluster_table");


    }

    @Test
    public void createTable1() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("cluster_table"));

        HColumnDescriptor hd = new HColumnDescriptor("f");
        hd.setDataBlockEncoding(DataBlockEncoding.DIFF);// 压缩算法
        hd.setBloomFilterType(BloomType.ROWCOL);//布隆过滤�?
        hd.setMaxVersions(3);//设置保存的版本数
        hd.setTimeToLive(Integer.MAX_VALUE);//设置TTL ,单位�?

        htd.addFamily(hd);
        hBaseAdmin.createTable(htd);
    }

    @Test
    public void createTable2() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table_desc_0002"));
        HColumnDescriptor hd = new HColumnDescriptor("f");

        htd.addFamily(hd);
        hBaseAdmin.createTable(htd, Bytes.toBytes(1), Bytes.toBytes(100),15);


        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("table_desc_0002"));
        //日志flush的时候是同步写，还是异步�?
        tableDesc.setDurability(Durability.SYNC_WAL);
        //MemStore大小
        tableDesc.setMemStoreFlushSize(256*1024*1024);

        HColumnDescriptor colDesc = new HColumnDescriptor("f");
        //块缓存，保存�?每个HFile数据块的startKey
        colDesc.setBlockCacheEnabled(true);
        //块的大小，默认�?�是65536
        //加载到内存当中的数据块越小，随机查找性能更好,越大，连续读性能更好
        colDesc.setBlocksize(64*1024);
        //bloom过滤器，有ROW和ROWCOL，ROWCOL除了过滤ROW还要过滤列族
        colDesc.setBloomFilterType(BloomType.ROW);
        //写的时�?�缓存bloom
        colDesc.setCacheBloomsOnWrite(true);
        //写的时�?�缓存索�?
        colDesc.setCacheIndexesOnWrite(true);
        //存储的时候使用压缩算�?
        colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
        //进行compaction的时候使用压缩算�?
        colDesc.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
        //压缩内存和存储的数据，区别于Snappy
        colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
        //写入硬盘的时候是否进行编�?
        colDesc.setEncodeOnDisk(true);
        //关闭的时候，是否剔除缓存的块
        colDesc.setEvictBlocksOnClose(true);
        //是否保存那些已经删除掉的kv
        colDesc.setKeepDeletedCells(false);
        //让数据块缓存在LRU缓存里面有更高的优先�?
        colDesc.setInMemory(true);
        //�?大最小版�?
        colDesc.setMaxVersions(3);
        colDesc.setMinVersions(1);
        //集群间复制的时�?�，如果被设置成REPLICATION_SCOPE_LOCAL就不能被复制�?
        colDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
        //生存时间
        colDesc.setTimeToLive(18000);

        tableDesc.addFamily(colDesc);


    }

    public void getHTableDescriptor() throws Exception{

        Configuration conf = HBaseConfiguration.create();
        HTableInterface table =  HConnectionManager.createConnection(conf).getTable(("table_desc_0001"));

        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        System.out.println(tableDescriptor.getTableName());

        System.out.println(tableDescriptor.getMaxFileSize());//maxStoreSize,限制region的大小，超过会拆分region
        System.out.println(tableDescriptor.getMemStoreFlushSize());//memStore刷写大小�?
        System.out.println(tableDescriptor.isReadOnly());



    }

    public void getHTableDescriptor2() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTableInterface table =  HConnectionManager.createConnection(conf).getTable(("table_desc_0001"));
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        System.out.println(tableDescriptor.getTableName());
        System.out.println(tableDescriptor);


        HColumnDescriptor[] hColumnDescriptors = tableDescriptor.getColumnFamilies();
        for(HColumnDescriptor hColumnDescriptor: hColumnDescriptors) {//获取列族

            System.out.println(hColumnDescriptor);
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

            System.out.println(hColumnDescriptor.getConfiguration());//配置
            System.out.println(hColumnDescriptor.getConfigurationValue("test"));

            System.out.println(hColumnDescriptor.getCompressionType());//压缩
            System.out.println(hColumnDescriptor.getCompression());
            System.out.println(hColumnDescriptor.getCompactionCompressionType());//合并压缩
            System.out.println(hColumnDescriptor.getCompactionCompression());

            System.out.println(hColumnDescriptor.getBloomFilterType());//布隆过滤�?,默认为行
            System.out.println(hColumnDescriptor.getBlocksize()/1024);//65536| 64K |块大�?

            System.out.println(hColumnDescriptor.getMaxVersions());//版本，默认为1
            System.out.println(hColumnDescriptor.getMinVersions());

            System.out.println(hColumnDescriptor.getName());//列族�?
            System.out.println(hColumnDescriptor.getNameAsString());

            System.out.println(hColumnDescriptor.getScope());//赋�?�范围，默认�?0不开�?
            System.out.println(hColumnDescriptor.getTimeToLive());//TTL，默认是�?大�??

            System.out.println(hColumnDescriptor.isBlockCacheEnabled());//块缓�?,与scan的setCacheBlocks�?致�?�默认开�?
            System.out.println(hColumnDescriptor.isLegalFamilyName(Bytes.toBytes("f")));//�?查列族是否存�?

            System.out.println(hColumnDescriptor.getMobThreshold());//



            System.out.println(hColumnDescriptor.getDataBlockEncoding());//数据压缩�?
            System.out.println(hColumnDescriptor.getEncryptionKey());
            System.out.println(hColumnDescriptor.getEncryptionType());

            System.out.println(hColumnDescriptor.getKeepDeletedCells());

            System.out.println(hColumnDescriptor.getValue(""));
            System.out.println(hColumnDescriptor.getValues());
        }

        Thread.sleep(10000L);
    }


}
