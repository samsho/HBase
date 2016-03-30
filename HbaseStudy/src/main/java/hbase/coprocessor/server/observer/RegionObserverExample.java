package hbase.coprocessor.server.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * ClassName: RegionObserverExample
 * Description:
 * Date: 2016/1/21 16:20
 *
 * @author sm12652
 * @version V1.0
 */
public class RegionObserverExample extends BaseRegionObserver{

    public static final Log LOG = LogFactory.getLog(RegionObserverExample.class);
    public static final byte[] FIXED_ROW = Bytes.toBytes("row");

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<KeyValue> result) throws IOException {
        LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));

        if (Bytes.equals(get.getRow(), FIXED_ROW)) {
            KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()));
            LOG.debug("Had a match, adding fake kv: " + kv);
            result.add(kv);

//            c.bypass();//一旦特殊的keyValue被添加，之后的操作都会被跳过。
        }
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {

        RegionCoprocessorEnvironment env = e.getEnvironment();
        HRegion hRegion = env.getRegion();

        RegionServerServices regionServerServices = env.getRegionServerServices();
        FlushRequester flushRequester = regionServerServices.getFlushRequester();
        flushRequester.requestFlush(hRegion);


    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        e.bypass();
    }
}
