package org.apache.hadoop.hive.ql.index.io;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.index.HiveIndex.HiveIndexResult;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class HiveIndexInputFormat extends HiveInputFormat {

  public static final Log l4j = LogFactory.getLog("HiveIndexInputFormat");

  public HiveIndexInputFormat() {
    super();
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String indexFileStr = job.get("hive.exec.index_file");
    l4j.info("index_file is " + indexFileStr);
    System.out.println("HiveIndexInputFormat::getSplits,index_file is " + indexFileStr);
    if (indexFileStr == null) {
      // ok it is not index accessed plan, what can we do?
      // return super.getSplits(job, numSplits);
      throw new IOException("No index file.");
    }
    HiveInputSplit[] splits = (HiveInputSplit[]) super
        .getSplits(job, numSplits);

    HiveIndexResult hiveIndexResult = null;
    try {
      hiveIndexResult = new HiveIndexResult(indexFileStr, job);
    } catch (HiveException e) {
      // there is
      l4j.error("Unable to read index so we will go with all the file splits.");
      e.printStackTrace();
    }

    ArrayList<HiveInputSplit> newSplits = new ArrayList<HiveInputSplit>(
        numSplits);
    for (HiveInputSplit split : splits) {
      l4j.info("split start : " + split.getStart());
      l4j.info("split end : " + (split.getStart() + split.getLength()));

      try {
        if (hiveIndexResult.contains(split)) {
          // we may miss a sync here
          HiveInputSplit newSplit = split;
          if (split.getStart() > RCFile.SYNC_INTERVAL) {
            newSplit = new HiveInputSplit(new FileSplit(split.getPath(), split
                .getStart()
                - RCFile.SYNC_INTERVAL, split.getLength()
                + RCFile.SYNC_INTERVAL, split.getLocations()), split
                .inputFormatClassName());
          }

          newSplits.add(newSplit);
        }
      } catch (HiveException e) {
        throw new RuntimeException(
            "Unable to get metadata for input table split" + split.getPath());
      }
    }
    InputSplit retA[] = newSplits.toArray((new FileSplit[newSplits.size()]));
    l4j.info("Number of input splits: " + splits.length + " new input splits: "
        + retA.length);
    return retA;
  }
}
package org.apache.hadoop.hive.ql.index.io;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.index.HiveIndex.HiveIndexResult;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class HiveIndexInputFormat extends HiveInputFormat {

  public static final Log l4j = LogFactory.getLog("HiveIndexInputFormat");

  public HiveIndexInputFormat() {
    super();
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String indexFileStr = job.get("hive.exec.index_file");
    l4j.info("index_file is " + indexFileStr);
    System.out.println("HiveIndexInputFormat::getSplits,index_file is " + indexFileStr);
    if (indexFileStr == null) {
      // ok it is not index accessed plan, what can we do?
      // return super.getSplits(job, numSplits);
      throw new IOException("No index file.");
    }
    HiveInputSplit[] splits = (HiveInputSplit[]) super
        .getSplits(job, numSplits);

    HiveIndexResult hiveIndexResult = null;
    try {
      hiveIndexResult = new HiveIndexResult(indexFileStr, job);
    } catch (HiveException e) {
      // there is
      l4j.error("Unable to read index so we will go with all the file splits.");
      e.printStackTrace();
    }

    ArrayList<HiveInputSplit> newSplits = new ArrayList<HiveInputSplit>(
        numSplits);
    for (HiveInputSplit split : splits) {
      l4j.info("split start : " + split.getStart());
      l4j.info("split end : " + (split.getStart() + split.getLength()));

      try {
        if (hiveIndexResult.contains(split)) {
          // we may miss a sync here
          HiveInputSplit newSplit = split;
          if (split.getStart() > RCFile.SYNC_INTERVAL) {
            newSplit = new HiveInputSplit(new FileSplit(split.getPath(), split
                .getStart()
                - RCFile.SYNC_INTERVAL, split.getLength()
                + RCFile.SYNC_INTERVAL, split.getLocations()), split
                .inputFormatClassName());
          }

          newSplits.add(newSplit);
        }
      } catch (HiveException e) {
        throw new RuntimeException(
            "Unable to get metadata for input table split" + split.getPath());
      }
    }
    InputSplit retA[] = newSplits.toArray((new FileSplit[newSplits.size()]));
    l4j.info("Number of input splits: " + splits.length + " new input splits: "
        + retA.length);
    return retA;
  }
}
