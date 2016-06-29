package com.hbase.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This is direct port (hopefully) of the Scala version of this class available
 *
 * @author
 */
public class HFileInputFormat extends FileInputFormat<ImmutableBytesWritable, KeyValue> {


	private class HFileRecordReader extends
			RecordReader<ImmutableBytesWritable, KeyValue> {

		private HFile.Reader reader;
		private final HFileScanner scanner;
		private int entryNumber = 0;

		public HFileRecordReader(FileSplit split, Configuration conf)
				throws IOException {
			SchemaMetrics.configureGlobally(conf);
			final Path path = split.getPath();
			reader = HFile.createReader(FileSystem.get(conf), path, new CacheConfig(conf));
			
			scanner = reader.getScanner(false, false);
			reader.loadFileInfo(); // This is required or else seekTo throws a
									// NPE
			scanner.seekTo(); // This is required or else scanner.next throws an
								// error
		}

		
		
		@Override
		public void close() throws IOException {
			if (reader != null) {
				reader.close();
			}
		}

		/*
		 * @Override public boolean next(ImmutableBytesWritable key, KeyValue
		 * value) throws IOException { entryNumber++; return scanner.next(); }
		 */

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new ImmutableBytesWritable(scanner.getKeyValue().getRow());
		}

		@Override
		public KeyValue getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return scanner.getKeyValue();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			entryNumber++;
			return scanner.next();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (reader != null) {
		        return (entryNumber / reader.getEntries());
		      }
		      return 1;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			
		}

	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
	    return false;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, KeyValue> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new HFileRecordReader((FileSplit) split,
				context.getConfiguration());
	}

}
