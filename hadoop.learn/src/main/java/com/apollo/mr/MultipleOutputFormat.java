package com.apollo.mr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This abstract class extends the FileOutputFormat, allowing to write the output data to different
 * output files. There are three basic use cases for this class. Created on 2012-07-08
 * @author zhoulongliu
 * @param <K>
 * @param <V>
 */
public abstract class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
    extends FileOutputFormat<K, V> {

  // 接口类，需要在调用程序中实现generateFileNameForKeyValue来获取文件名
  private MultiRecordWriter writer = null;

  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException,
      InterruptedException {
    if (writer == null) {
      writer = new MultiRecordWriter(job, getTaskOutputPath(job));
    }
    return writer;
  }

  /**
   * get task output path
   * @param conf
   * @return
   * @throws IOException
   */
  private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
    Path workPath = null;
    OutputCommitter committer = super.getOutputCommitter(conf);
    if (committer instanceof FileOutputCommitter) {
      workPath = ((FileOutputCommitter) committer).getWorkPath();
    } else {
      Path outputPath = super.getOutputPath(conf);
      if (outputPath == null) {
        throw new IOException("Undefined job output-path");
      }
      workPath = outputPath;
    }
    return workPath;
  }

  /**
   * 通过key, value, conf来确定输出文件名（含扩展名） Generate the file output file name based on the given key and
   * the leaf file name. The default behavior is that the file name does not depend on the key.
   * @param key the key of the output data
   * @param name the leaf file name
   * @param conf the configure object
   * @return generated file name
   */
  protected abstract String generateFileNameForKeyValue(K key, V value, Configuration conf);

  /**
   * 实现记录写入器RecordWriter类 （内部类）
   * @author zhoulongliu
   */
  public class MultiRecordWriter extends RecordWriter<K, V> {
    /** RecordWriter的缓存 */
    private HashMap<String, RecordWriter<K, V>> recordWriters = null;
    private TaskAttemptContext job = null;
    /** 输出目录 */
    private Path workPath = null;

    public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
      super();
      this.job = job;
      this.workPath = workPath;
      recordWriters = new HashMap<String, RecordWriter<K, V>>();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();
      while (values.hasNext()) {
        values.next().close(context);
      }
      this.recordWriters.clear();
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      // 得到输出文件名
      String baseName = generateFileNameForKeyValue(key, value, job.getConfiguration());
      // 如果recordWriters里没有文件名，那么就建立。否则就直接写值。
      RecordWriter<K, V> rw = this.recordWriters.get(baseName);
      if (rw == null) {
        rw = getBaseRecordWriter(job, baseName);
        this.recordWriters.put(baseName, rw);
      }
      rw.write(key, value);
    }

    // ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}
    private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName)
        throws IOException, InterruptedException {
      Configuration conf = job.getConfiguration();
      // 查看是否使用解码器
      boolean isCompressed = getCompressOutput(job);
      String keyValueSeparator = ",";
      RecordWriter<K, V> recordWriter = null;
      if (isCompressed) {
        Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
          GzipCodec.class);
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
        Path file = new Path(workPath, baseName + codec.getDefaultExtension());
        FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
        // 这里我使用的自定义的OutputFormat
        recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(
            codec.createOutputStream(fileOut)), keyValueSeparator);
      } else {
        Path file = new Path(workPath, baseName);
        FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
        // 这里我使用的自定义的OutputFormat
        recordWriter = new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
      }
      return recordWriter;
    }
  }

}