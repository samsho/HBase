package hbase.coprocessor.server.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class rowCountObserver extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(rowCountObserver.class);

    RegionCoprocessorEnvironment env;
    HRegion m_region;
    ZooKeeperWatcher zkw;
    String zNodePath;
    long myrowcount = 0; //count;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
        RegionServerServices rss = env.getRegionServerServices();
        m_region = env.getRegion();

        zNodePath = zNodePath + m_region.getRegionNameAsString();
        zkw = rss.getZooKeeper();
        try {
            if (ZKUtil.checkExists(zkw, zNodePath) == -1) {
                logger.error("LIULIUMI: cannot find the znode");
                ZKUtil.createWithParents(zkw, zNodePath);
                logger.info("znode path is : " + zNodePath);
            }
        } catch (Exception ee) {
            logger.error("LIULIUMI: create znode fail");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }


    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        long count = 0;
        //Scan 获取当前 region 保存的行数
        try {
            Scan scan = new Scan();
            InternalScanner scanner = null;
            scanner = m_region.getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            do {
                hasMore = scanner.next(results);
                if (results.size() > 0)
                    count++;
            } while (hasMore);

            //用当前的行数设置 ZooKeeper 中的计数器初始值
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(count));
            //设置 myrowcount 类成员，用来表示当前 Region 的 rowcount
            myrowcount = count;
        } catch (Exception ex) {
            logger.info("setData exception",ex);
        }

    }

}
