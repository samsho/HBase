package hbase.tableadmin.region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * ClassName: HBaseRegionTest
 * Description:
 * Date: 2015/12/15 10:09
 *
 * @author sm12652
 * @version V1.0
 */
public class HBaseRegionTest {

    static HBaseAdmin hBaseAdmin;
    static Configuration conf;

    @Before
    public void before() {
        conf = HBaseConfiguration.create();
        try {
            hBaseAdmin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * HBaseAdmin 的各方法
     * @throws Exception
     */
    @Test
    public void method() throws Exception {
        System.out.println(hBaseAdmin);

        hBaseAdmin.checkHBaseAvailable(conf);//验证客户端应用是否能与远程HBase服务通信

        hBaseAdmin.getClusterStatus();//获取集群信息

        // 下线某个region,�?要准确的regionname.这个命令不经过master
        // 直接与region服务器�?�信
        hBaseAdmin.closeRegion("", "");

        hBaseAdmin.flush("");//实时刷写

        hBaseAdmin.compact("");//异步,合并
        hBaseAdmin.majorCompact("");//异步，大合并

        hBaseAdmin.split("");//异步，分�?

        //基于master的控制region的上线�?�下线；
        hBaseAdmin.assign(null);
        hBaseAdmin.unassign(null, true);//final boolean force,true-强制下线

        //移动某个region到某个regionserver�?
        // destServerName为空，则随机移动
        hBaseAdmin.move(null,null);

        hBaseAdmin.balancer();//负载均衡

        // 关闭
        hBaseAdmin.shutdown();//关闭集群
        hBaseAdmin.stopMaster();//关闭master
        hBaseAdmin.stopRegionServer("");//关闭某台regionserver
    }

    /**
     * 获取集群信息
     * @throws Exception
     */
    @Test
    public void getClusterStatus() throws Exception {

        ClusterStatus  clusterStatus =  hBaseAdmin.getClusterStatus();//获取集群信息

        ServerName master =  clusterStatus.getMaster();


        System.out.println(clusterStatus);

        System.out.println(clusterStatus.getServersSize());//活着的region数量
//        System.out.println(clusterStatus.getServers());//活着的region列表

        System.out.println(clusterStatus.getDeadServers());//挂掉的
//        System.out.println(clusterStatus.getDeadServerNames());


        System.out.println(clusterStatus.getAverageLoad());//平均每台regionserver上线了多少个region
        System.out.println(clusterStatus.getRegionsCount());//集群region总数
        System.out.println(clusterStatus.getRegionsInTransition());//集群正在处理的region事务列表

        System.out.println(clusterStatus.getRequestsCount());//集群请求的tps

        System.out.println(clusterStatus.getHBaseVersion());//HBase的版本
        System.out.println(clusterStatus.getVersion());//clusterStatus的版本号

        System.out.println(clusterStatus.getClusterId());//集群编号，集群第1次启动时候的UUID

        for (ServerName serverName : clusterStatus.getServers()) {
            //某个个server
            System.out.println("活着的集群名称" + serverName);
            System.out.println(serverName.getHostAndPort());//域名与RPC端口合并的字符串
            System.out.println(serverName.getHostname());//域名或IP
            System.out.println(serverName.getStartcode());//服务器启动时间
//            System.out.println(serverName.getServerName());//
            System.out.println(serverName.getPort());

            // 当前server的负载
            // 返回给定region服务器的当前负载状况
            ServerLoad serverLoad = clusterStatus.getLoad(serverName);
            System.out.println("集群" + serverName + "的负载 ： " + serverLoad);
            System.out.println(serverLoad.getLoad());//同getNumberOfRegions
            System.out.println("当前+"+ serverName + "+服务器上线的region数量" + serverLoad.getNumberOfRegions());//当前region服务器上线的region数量

            //当前regionserver这个周期内的tps
            // 周期可以通过hbase.regionserver.msginterval来设定
            System.out.println(serverLoad.getNumberOfRequests());
            System.out.println(serverLoad.getTotalNumberOfRequests());

            System.out.println(serverLoad.getUsedHeapMB());//JVM已经使用的内存
            System.out.println(serverLoad.getMaxHeapMB());// jvm�?大可使用内存

            System.out.println(serverLoad.getStorefiles());//当前regionserver的存储文件储
            System.out.println(serverLoad.getStorefileSizeInMB());//总存储量，单位是MB
            System.out.println(serverLoad.getStorefileIndexSizeInMB());//当前regionserver的存储文件的索引大小
            System.out.println(serverLoad.getMemstoreSizeInMB());//当前regionserver的已用的写缓存大
            System.out.println(serverLoad.getRegionsLoad());// 返回每个region的负载情�?.key为region名，值是



            for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
//                entry.getKey();
                RegionLoad regionLoad = entry.getValue();
                System.out.println(entry.getKey());
                System.out.println(regionLoad);

                System.out.println(regionLoad.getName());
                System.out.println(regionLoad.getNameAsString());
                System.out.println(regionLoad.getStores());
                System.out.println(regionLoad.getStorefiles());
                System.out.println(regionLoad.getStorefileSizeMB());

                System.out.println(regionLoad.getStorefileIndexSizeMB());
                System.out.println(regionLoad.getMemStoreSizeMB());
                System.out.println(regionLoad.getRequestsCount());
                System.out.println(regionLoad.getReadRequestsCount());
                System.out.println(regionLoad.getWriteRequestsCount());

            }
        }
    }


    /**
     * 获取表信息
     * @throws IOException
     */
    public void getTableInfo() throws IOException {
        //http://hmaster:60010/table.jsp?name=AreaInfo
        //HMaster master 怎么拿？？

//        HMaster master =
        Map<ServerName, Integer> regDistribution = new TreeMap<ServerName, Integer>();
        HTable table = (HTable) HConnectionManager.createConnection(conf).getTable(("table_desc_0001"));
        Map<HRegionInfo, ServerName> regions = table.getRegionLocations();
        HRegionInfo meta = HRegionInfo.FIRST_META_REGIONINFO;
        meta.getStartKey();
        meta.getEndKey();

        for (Map.Entry<HRegionInfo, ServerName> hriEntry : regions.entrySet()) {
            HRegionInfo regionInfo = hriEntry.getKey();
            ServerName addr = hriEntry.getValue();
            long req = 0;

            String urlRegionServer = null;

            if (addr != null) {

//                ServerLoad sl = master.getServerManager().getLoad(addr);
                ServerLoad sl = hBaseAdmin.getClusterStatus().getLoad(addr);
                if (sl != null) {
                    Map<byte[], RegionLoad> map = sl.getRegionsLoad();
                    if (map.containsKey(regionInfo.getRegionName())) {
                        req = map.get(regionInfo.getRegionName()).getRequestsCount();
                    }
                    Integer i = regDistribution.get(addr);
                    if (null == i) i = Integer.valueOf(0);
                    regDistribution.put(addr, i + 1);
                }
            }

        }
    }
}
