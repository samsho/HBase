package hbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

import java.io.*;
import java.security.PrivilegedExceptionAction;

/**
 * ClassName: HDFSClient
 * Description: HDFS公共操作类
 * date: 2015-9-1 下午4:39:51
 *
 * @author sm12652
 * @version V1.0
 * @since JDK 1.7
 */
public class HDFSClient {

    public static FileSystem newInstance(final Configuration conf) throws IOException, InterruptedException {
        return newInstance(conf, "hadoop");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static FileSystem newInstance(final Configuration conf, String user) throws IOException,
            InterruptedException {
        String ticketCachePath = conf.get("hadoop.security.kerberos.ticket.cache.path");
        UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, user);
        return (FileSystem) ugi.doAs(new PrivilegedExceptionAction() {
            public FileSystem run() throws IOException {
                return FileSystem.newInstance(conf);
            }
        });
    }

    /**
     * 是否存在
     *
     * @param conf
     * @param path
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean exists(final Configuration conf, String path) throws IOException, InterruptedException {
        FileSystem fs = newInstance(conf);
        Path dstPath = new Path(path);
        return fs.exists(dstPath);
    }

    /**
     * 彻底删除文件目录
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean delete(final Configuration conf, String path) throws IOException, InterruptedException {
        FileSystem fileSystem = newInstance(conf);
        Path dstPath = new Path(path);
        boolean flag = false;
        if (fileSystem.exists(dstPath)) {
            flag = fileSystem.delete(dstPath, true);
        }
        fileSystem.close();
        return flag;
    }

    /**
     * 新建文件
     *
     * @param conf
     * @param file
     * @param targetPath
     * @throws Exception
     */
    public static void create(final Configuration conf, String file, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path dst = new Path(targetPath + "/" + file);
        fileSystem.create(dst);
        fileSystem.close();
    }

    /**
     * 创建目录
     *
     * @param conf
     * @param filePath
     * @param targetPath
     * @throws Exception
     */
    public static void mkdir(final Configuration conf, String filePath, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path dst = new Path(targetPath + "/" + filePath);
        fileSystem.mkdirs(dst);
        fileSystem.close();
    }

    /**
     * 上传
     *
     * @throws Exception
     */
    public static void copyFromLocal(final Configuration conf, String sourcePath, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path src = new Path(sourcePath);
        Path dst = new Path(targetPath);
        fileSystem.copyFromLocalFile(src, dst);
        fileSystem.close();
    }

    /**
     * @param conf
     * @param sourcePath
     * @param targetPath
     * @throws Exception
     */
    public static void copyToLocal(final Configuration conf, String sourcePath, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path source = new Path(sourcePath);
        Path target = new Path(targetPath);
        fileSystem.copyToLocalFile(source, target);
    }


    /**
     * 读取本地文件到HDFS系统<br>,处理word文档
     * @param conf
     * @param in
     * @param targetPath
     * @throws Exception
     */
    public static void uploadWord(final Configuration conf, InputStream in, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path f = new Path(targetPath);

        FSDataOutputStream os = fileSystem.create(f);
        XWPFDocument doc = new XWPFDocument(in);//处理word文档
        doc.write(os);
        os.close();
    }

    /**
     * 读取本地文件到HDFS系统
     * @param conf
     * @param in
     * @param targetPath
     * @throws Exception
     */
    public static void upload(final Configuration conf, InputStream in, String targetPath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path f = new Path(targetPath);

        InputStreamReader isr = new InputStreamReader(in, "UTF-8");
        BufferedReader br = new BufferedReader(isr);

        FSDataOutputStream os = fileSystem.create(f);
        Writer out = new OutputStreamWriter(os, "UTF-8");

        String str = "";
        while ((str = br.readLine()) != null) {
            out.write(str + "\n");
        }
        br.close();
        isr.close();
        out.close();
        os.close();
    }

    /**
     * 下载文件
     *
     * @param conf
     * @param sourcePath
     * @throws Exception
     */
    public static InputStream downLoad(final Configuration conf, String sourcePath) throws Exception {
        FileSystem fileSystem = newInstance(conf);
        Path dst = new Path(sourcePath);
        return fileSystem.open(dst);
    }

}
