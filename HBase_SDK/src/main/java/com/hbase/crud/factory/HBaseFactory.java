package com.hbase.crud.factory;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hbase 工厂
 * 
 * @author wb6214
 * 
 */
public class HBaseFactory {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseFactory.class);
    static final Map<String, HConnection> CONNECTION_INSTANCES;
    static final Map<String, HBaseAdmin> ADMIN_INSTANCES;


    static {
        CONNECTION_INSTANCES = new HashMap<String, HConnection>();
        ADMIN_INSTANCES = new HashMap<String, HBaseAdmin>();
    }

    public static Configuration getConfiguration(String path) {
        FileInputStream in = null;
        try {
            in = new FileInputStream(path);
        } catch (FileNotFoundException e) {
            logger.error("获取配置失败，配置文件不存在", e);
        }

        Configuration conff = HBaseConfiguration.create(new Configuration());
        conff.addResource(in);
        return conff;
    }

    /**
     * 获取链接
     * @param path
     * @return
     */
    public static HConnection getConnection(String path) throws IOException {
        synchronized (CONNECTION_INSTANCES) {
            HConnection connection = CONNECTION_INSTANCES.get(path);
                if (connection == null) {
                    connection = HConnectionManager.createConnection(getConfiguration(path));
                    CONNECTION_INSTANCES.put(path, connection);
                }
            return connection;
        }
    }

    public static HBaseAdmin getHBaseAdmin(String path) {
        synchronized (ADMIN_INSTANCES) {
            HBaseAdmin hBaseAdmin = ADMIN_INSTANCES.get(path);
            try {
                if (hBaseAdmin == null) {
                    hBaseAdmin = new HBaseAdmin(getConfiguration(path));
                    ADMIN_INSTANCES.put(path, hBaseAdmin);
                }
            } catch (IOException e) {
                logger.error("创建hbase连接失败", e);
            }
            return hBaseAdmin;
        }
    }

    /**
     * 
     * Title: getHTable
     * Description: T获取hbase表的实例化对�? 此方法建议用于读取数据时
     * date: 2015�?4�?30�? 上午11:14:46
     * 
     * @param tableName
     * @return
     * @throws Exception
     * @author sz10686
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static HTableInterface getHTable(String path,String tableName) throws Exception {
        return getConnection(path).getTable(tableName);
    }

    /**
     * 
     * Title: getHTable
     * Description: 获取hbase表的实例化对�? 此方法建议用于写数据�?
     * date: 2015�?4�?30�? 上午11:15:15
     * 
     * @param tableName
     * 表名�?
     * @param autoFlsh
     * 是否�?要自动提交，建议赋�?�为false�?
     * @return 表的实例
     * @throws Exception
     * @author sz10686
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static HTableInterface getHTable(String path, String tableName, boolean autoFlsh) throws Exception {
        HTableInterface table = getConnection(path).getTable(tableName);
        table.setAutoFlushTo(autoFlsh);
        return table;
    }

    /**
     * 关闭表，关闭此流并释放与此流关联的所有系统资源，建议表查询之后就调用此方法以便及时的释放内存
     */
    public static void closeHTable(HTableInterface hTableInterface) {
        try {
            Closeables.close(hTableInterface, true);
        } catch (IOException e) {
            logger.error("关闭表失�?", e);
        }
    }

    /**
     * 
     * Title: deleteTable
     * Description: 删除�?
     * date: 2015�?5�?12�? 下午12:13:27
     * 
     * @param tableName
     * @return true:成功关闭 false�? 未成功关�?
     * @throws Exception
     * @author sz10686
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static boolean deleteTable(String path, String tableName) throws Exception {
        // 判断表是否存在，避免不必要的异常出现
        HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        if (hBaseAdmin.tableExists(tableName)) {
            disable(path, tableName);
            hBaseAdmin.deleteTable(tableName);
        }
        return true;
    }

    /**
     * 
     * Title: deleteTable
     * Description: 启用�?
     * date: 2015�?5�?6�? 下午3:12:06
     * 
     * @param tableName
     * @return
     * @throws Exception
     * @author sz10686
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static boolean enable(String path, String tableName) throws Exception {
        // 判断表是否可用，避免不必要的异常出现
        HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        boolean isDisabled = hBaseAdmin.isTableDisabled(tableName);
        if (isDisabled) {
            // 如果不可用，则启用表
            hBaseAdmin.enableTable(tableName);
        }
        return true;
    }

    /**
     * 
     * Title: disable
     * Description: 禁用�?
     * date: 2015�?5�?6�? 下午3:18:35
     * 
     * @param tableName
     * @return
     * @throws Exception
     * @author sz10686
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static boolean disable(String path, String tableName) throws Exception {
        // 判断表是否可用，避免不必要的异常出现
        HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        boolean isDisabled = hBaseAdmin.isTableDisabled(tableName);
        if (!isDisabled) {
            // 如果不可用，则启用表
            hBaseAdmin.disableTable(tableName);
        }
        return true;
    }

    public static void destroy() throws Exception {
        for (Map.Entry<String, HConnection> entry : CONNECTION_INSTANCES.entrySet()) {
            Closeables.close(entry.getValue(), true);
        }

        for (Map.Entry<String, HBaseAdmin> entry : ADMIN_INSTANCES.entrySet()) {
            Closeables.close(entry.getValue(), true);
        }
    }

    /**
     * 
     * @param overwrit
     * : 表存在的话是否覆盖？true：覆�? false：不覆盖
     * @param tableName
     * ：表�?
     * @param family
     * �? 列族
     * @param encoding
     * ：缓存的压缩算法，枚举�?�推荐：DataBlockEncoding.DIFF算法
     * @param onDisk
     * ：true:
     * @param cacheEnabled
     * @param inMemory
     * @param bType
     * @throws Exception
     */
    public void createTable(String path,Boolean overwrit, String tableName, String family, DataBlockEncoding encoding,
            Boolean onDisk, Boolean cacheEnabled, Boolean inMemory, BloomType bType) throws Exception {
        // 判断是否覆盖
        HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        if (overwrit) {
            // �?测表是否存在�?
            try {
                if (hBaseAdmin.tableExists(tableName)) {
                    hBaseAdmin.disableTable(tableName);
                    hBaseAdmin.deleteTable(tableName);
                }
            } catch (Exception e) {
                logger.error("创建表失�?", e);
            }
        } else {
            if (hBaseAdmin.tableExists(tableName)) {
                return;
            }
        }
        HColumnDescriptor hd = new HColumnDescriptor(family);
        hd.setDataBlockEncoding(encoding);
        // hd.setBlockCacheEnabled(cacheEnabled);
        /**
         * 为true表示：StoreFile在创建Reader时会创建�?个BlockCache对象(默认为LruBlockCache),
         * reader读取数据�?,先从BlockCache中读�?,如果缓存中没�?,而且本次读取的结果可以缓�?,
         * 那么把结�?(CachedBlock)缓存到BlockCache�?.
         */
        hd.setBloomFilterType(bType); // 空间换时间，适用于随机读取（get），可以提高效率
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        htd.addFamily(hd);
        hBaseAdmin.createTable(htd);
        hBaseAdmin.flush(tableName);
        destroy(); // 释放内存
    }

    /**
     * 判断表是否存�?
     * 
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(String path,String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        return hBaseAdmin.tableExists(tableName);
    }

    /**
     * 创建HBase�?(预分割Region个数)
     * @throws IOException
     */
    public static void createTable(String path) throws Exception {

        /*HBaseAdmin hBaseAdmin = getHBaseAdmin(path);
        // 支持多个family，以（，）分�?
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        if (!Strings.isNullOrEmpty(family)) {
            String[] families = family.split(CommonConstants.SPLIT);
            if (null != families && families.length > 0) {
                HColumnDescriptor hd = null;
                for (String fam : families) {
                    hd = new HColumnDescriptor(fam);
                    hd.setDataBlockEncoding(DataBlockEncoding.DIFF);// 压缩算法
                    hd.setBloomFilterType(BloomType.ROWCOL);
                    hd.setMaxVersions(CommonConstants.HBASE_MAX_VERSIONS);//设置保存的版本数,modify by sm12652 20150811
                    hd.setTimeToLive(ttl);//设置TTL modify by sm12652 20150813(以后如果多列族，�?要�?�当调整)
                    htd.addFamily(hd);
                }
            }
        }

        byte[][] splits = null;
        // 不分�?
        if (!obj.isSplit()) {
            hBaseAdmin.createTable(htd);
        } else {
        	// 分割，不使用MD5
            if (!obj.isMd5()) {
                splits = getHexSplitsBySelf(obj.getStartKey(), obj.getEndKey(), obj.getSplitNum());
            } else {//分割，使用MD5
                splits = getHexSplitsMd5(obj.getSplitNum(), false);
            }
            hBaseAdmin.createTable(htd, splits);
        }
        hBaseAdmin.flush(tableName);*/
    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey);
        BigInteger highestKey = new BigInteger(endKey);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = (key + "").getBytes();
            splits[i] = b;
        }
        return splits;
    }


    public static byte[][] getHexSplit(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions-1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for(int i=0; i < numRegions-1;i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    public static String path(String colonyId) {
        return colonyId+"/hbase-site.xml";
    }

    /**
     * 根据md5密文进行自定义预分割
     * @param numRegions
     * @return
     */
    public static byte[][] getHexSplitsMd5(int numRegions, boolean isChar) {

        String[] hexDigits = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };
        if (isChar) {
            String[] hexDigits2 = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f",
                    "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
            hexDigits = hexDigits2;
        }
        int mult = numRegions / hexDigits.length;
        if (numRegions % hexDigits.length != 0) {
            mult = mult + 1;
        }
        String hexDigitsT[] = new String[mult * hexDigits.length];
        if (numRegions > hexDigits.length) {
            for (int i = 0; i < hexDigits.length; i++) {
                int flage = hexDigits.length / mult;
                for (int j = 0; j < mult; j++) {
                    if (j == 0) {
                        hexDigitsT[mult * i] = hexDigits[i] + hexDigits[j];
                    } else {
                        hexDigitsT[mult * i + j] = hexDigits[i] + hexDigits[flage];
                        flage += hexDigits.length / mult;
                    }
                }
            }
            hexDigits = hexDigitsT;
        }
        byte[][] splits = new byte[numRegions - 1][];
        int all = hexDigits.length;
        int step = all / numRegions;
        int temp = step;
        for (int i = 0; i < numRegions - 1; i++) {
            String tt = hexDigits[temp];
            splits[i] = String.valueOf(tt).getBytes();
            temp += step;
        }
        return splits;
    }
    
    /**
     * 
     * Title: 
     * Description: 自定义分割加�?
     * date: 2015�?8�?6�? 下午2:24:45
     * 
     * @param numRegions 分割数量
     * @param startKey �?始分隔符
     * @param endKey 结束分隔�?
     * @return
     * @author sm12652
     * Modify History
     * User | Date | Description
     * -------------------------
     */
    public static byte[][] getHexSplitsBySelf(String startKey, String endKey,int numRegions) {

    	String[] hexDigitsModel = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f",
                "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
        
    	//截取在两个key之间的�?�，形成新的数组
    	List<String> list = Lists.newArrayList();
    	for (String valueKey : hexDigitsModel) {
        	if(valueKey.compareToIgnoreCase(startKey) >=0 && valueKey.compareToIgnoreCase(endKey)<=0 ){
        		list.add(valueKey);
        	}
    	}
    	int size = list.size();
    	String[] hexDigits = (String[])list.toArray(new String[size]); 
        
        int mult = numRegions / size;
        if (numRegions % size != 0) {
            mult = mult + 1;
        }
        String hexDigitsT[] = new String[mult * size];
        if (numRegions > size) {
            for (int i = 0; i < size; i++) {
                int flage = size / mult;
                for (int j = 0; j < mult; j++) {
                    if (j == 0) {
                        hexDigitsT[mult * i] = hexDigits[i] + hexDigits[j];
                    } else {
                        hexDigitsT[mult * i + j] = hexDigits[i] + hexDigits[flage];
                        flage += size / mult;
                    }
                }
            }
            hexDigits = hexDigitsT;
        }
        byte[][] splits = new byte[numRegions - 1][];
        int all = hexDigits.length;
        int step = all / numRegions;
        int temp = step;
        for (int i = 0; i < numRegions - 1; i++) {
            String tt = hexDigits[temp];
            splits[i] = String.valueOf(tt).getBytes();
            temp += step;
        }
        return splits;
    }
    
}