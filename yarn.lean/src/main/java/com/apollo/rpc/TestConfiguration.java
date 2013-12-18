package com.apollo.rpc;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

public class TestConfiguration {
  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    final JobConf defaultConf = new JobConf();
    long end1 = System.currentTimeMillis();
    defaultConf.addResource("job.xml");
    long end2 = System.currentTimeMillis();
    UserGroupInformation.setConfiguration(defaultConf);
    long end3 = System.currentTimeMillis();
    System.out.println("spent time:" + (end1 - start) + "," + (end2 - end1) + "," + (end3 - end2));
    System.exit((int) (end1-start));
  }
}
