package hbase.mr;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * file to table
 * 从文本到HBase表
 *
 */
public class ImportFromFile {
    private static final Log LOG = LogFactory.getLog(ImportFromFile.class);

    public static final String NAME = "ImportFromFile"; // co ImportFromFile-1-Name Define a job name for later use.

    public enum Counters {LINES}

    static class ImportMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {

        private byte[] family = null;
        private byte[] qualifier = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if (colkey.length > 1) {
                qualifier = colkey[1];
            }
        }

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException {
            try {
                String lineString = line.toString();
                byte[] rowkey = DigestUtils.md5(lineString);
                Put put = new Put(rowkey);
                put.add(family, qualifier, Bytes.toBytes(lineString));
                context.write(new ImmutableBytesWritable(rowkey), put);
                context.getCounter(Counters.LINES).increment(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option o = new Option("t", "table", true,
                "table to import into (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("c", "column", true,
                "column to store row data into (must exist)");
        o.setArgName("family:qualifier");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("i", "input", true,
                "the directory or file to read from");
        o.setArgName("path-in-HDFS");
        o.setRequired(true);
        options.addOption(o);
        options.addOption("d", "debug", false, "switch on DEBUG log level");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
        }
        return cmd;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);

        if (cmd.hasOption("d")) conf.set("conf.debug", "true");

        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");
        conf.set("conf.column", column);

        Job job = Job.getInstance(conf, "Import from file " + input +
                " into table " + table);
        job.setJarByClass(ImportFromFile.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
