package com.apollo.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.apollo.mr.WordCount.Qc1Comparator;
import com.apollo.util.Bytes;
import com.google.common.net.InetAddresses;

/**
 * @author dragon.caol
 */
public class AnalyzeMonitorDataOld {

  public class MyPartitioner implements Partitioner<Text, Text> {

    @Override
    public void configure(JobConf arg0) {
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
      return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class Mapperq extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key1 = new Text();
    private final Text value1 = new Text();
    private final SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
    private final static char HIVE_HEX = '\u0001';

    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String[] splits = value.toString().split("\t");
      String keyValue = splits[0];
      byte[] keys = null;
      try {
        keys = Hex.decodeHex(keyValue.toCharArray());
      } catch (DecoderException e) {
        e.printStackTrace();
      }
      String end_time = String.valueOf(Long.valueOf(splits[1]));
      String pool_id = splits[2];
      Inet4Address dst_ip1 = InetAddresses.fromInteger(Integer.valueOf(splits[3]));
      String dst_ip = dst_ip1.getHostAddress();
      String length = splits[4];
      String type = splits[5];
      String path = splits[6];

      long stringTimeL = Long.MAX_VALUE - Bytes.toLong(keys, 4, 8);
      String startTime = String.valueOf(stringTimeL);
      String block_id = String.valueOf(Bytes.toLong(keys, 12, 8));
      Inet4Address client_ip1 = InetAddresses.fromInteger(Bytes.toInt(keys, 20, 4));
      String client_ip = client_ip1.getHostAddress();
      String namenode = convertNamenode(Bytes.toLong(keys, 24, 8)).getHostName();
      String day = ft.format(new Date(stringTimeL));

      StringBuilder sb = new StringBuilder();
      sb.append(startTime).append(HIVE_HEX);
      sb.append(end_time).append(HIVE_HEX);
      sb.append(pool_id).append(HIVE_HEX);
      sb.append(dst_ip).append(HIVE_HEX);
      sb.append(length).append(HIVE_HEX);
      sb.append(type).append(HIVE_HEX);
      sb.append(path).append(HIVE_HEX);
      sb.append(block_id).append(HIVE_HEX);
      sb.append(client_ip).append(HIVE_HEX);
      sb.append(namenode).append(HIVE_HEX);
      sb.append(day);
      value1.set(sb.toString());
      key1.set(day + ":" + (int) (Math.random() * 20));
      context.write(key1, value1);
    }
  }

  public static class Reducerq extends Reducer<Text, Text, NullWritable, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      for (Text val : values) {
        context.write(NullWritable.get(), val);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: AnalyzeMonitorData <in> <out>");
      System.exit(2);
    }

    conf.setOutputKeyComparatorClass(Qc1Comparator.class);
    Job job = new Job(conf, "dragon");
    job.setJarByClass(AnalyzeMonitorDataOld.class);

    job.setMapperClass(Mapperq.class);
    job.setReducerClass(Reducerq.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(300);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main2(String args[]) {
    File file = new File("/tmp/test_hbase_monitor");
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String tempString = null;
      int line = 1;
      // 一次读入一行，直到读入null为文件结束
      while ((tempString = reader.readLine()) != null) {
        // 显示行号
        System.out.println("line " + line + ": " + tempString);
        line++;
      }
      reader.close();
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
        }
      }
    }
  }

  public static InetSocketAddress convertNamenode(long namenode) {
    int ip = (int) (namenode / 100000);
    int port = (int) (namenode % 100000);
    if (port < 0) port += 100000;

    return new InetSocketAddress(InetAddresses.fromInteger(ip), port);
  }
}
