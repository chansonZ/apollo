package com.apollo.hadoop2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestSplit2 {
  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern
      .compile("^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s"
          + "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)"
          + "(\\s[0-9-]+){15}");
  private static final String PROCFS = "/proc/";

  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < Integer.parseInt(args[0]); i++) {
      getProcessTree();
    }
    System.out.println("ProcessTree:" + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    for (int j = 0; j < 3196; j++) {
      for (int i = 0; i < 1000; i++) {
        String str = "asas asa s asas asas asa sa sas as as as a a"
            + "asa sasa sa sa sas as as asas as as as as as as"
            + "as as a sas asdasdas dasd asda sd ada d";
        str.toString().split(" ");
      }
    }
    System.out.println("split:" + (System.currentTimeMillis() - start));
  }

  public static void getProcessTree() {
    // Get the list of processes
    List<Integer> processList = getProcessList();
    // System.out.println("p:" + processList.size());
    for (Integer proc : processList) {
      constructProcessInfo(proc);
    }
  }

  private static void constructProcessInfo(int pid) {
    // Read "/proc/<pid>/stat" file
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      fReader = new FileReader(PROCFS + pid + "/stat");
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // The process vanished in the interim!
    }

    try {
      String str = in.readLine(); // only one line
      Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
      boolean mat = m.find();
      if (mat) {
        // Set ( name ) ( ppid ) ( pgrpId ) (session ) (vsize )
        m.group(2);
        Integer.parseInt(m.group(3));
        Integer.parseInt(m.group(4));
        Integer.parseInt(m.group(5));
        Long.parseLong(m.group(7));
      } else {
      }
    } catch (IOException io) {
    } finally {
      // Close the streams
      try {
        if (fReader != null) {
          fReader.close();
        }
        try {
          if (in != null) {
            in.close();
          }
        } catch (IOException i) {
        }
      } catch (IOException i) {
      }
    }
  }

  /**
   * Get the list of all processes in the system.
   */
  private static List<Integer> getProcessList() {
    String[] processDirs = (new File(PROCFS)).list();
    List<Integer> processList = new ArrayList<Integer>();

    for (String dir : processDirs) {
      try {
        int pd = Integer.parseInt(dir);
        if ((new File(PROCFS + dir)).isDirectory()) {
          processList.add(Integer.valueOf(pd));
        }
      } catch (NumberFormatException n) {
        // skip this directory
      } catch (SecurityException s) {
        // skip this process
      }
    }
    return processList;
  }

}