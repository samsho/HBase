package hbase.coprocessor.server.observer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;

/**
 * Created by home on 2016/1/23.
 */
public class MasterObserverExample extends BaseMasterObserver {

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
//        super.postCreateTable(ctx, desc, regions);
        String tName = regions[0].getTable().getNameAsString();

        MasterServices masterServices = ctx.getEnvironment().getMasterServices();
        FileSystem fileSystem = masterServices.getMasterFileSystem().getFileSystem();
        Path path = new Path(tName + "-path");
        fileSystem.mkdirs(path);
    }
}
