/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.index;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class IndexBuilderFileFormat<K extends LongWritable, V extends Writable>
    extends HiveInputFormat<K, V> {

  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    HiveInputSplit hsplit = (HiveInputSplit) split;

    InputSplit inputSplit = hsplit.getInputSplit();
    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = Class.forName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    boolean blockPointer = false;
    long blockStart = -1;    
    FileSplit fileSplit = (FileSplit) split;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(job);
    if (inputFormatClass.getName().contains("SequenceFile")) {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, path, job);
      blockPointer = in.isBlockCompressed();
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    } else if (inputFormatClass.getName().contains("RCFile")) {
      RCFile.Reader in = new RCFile.Reader(fs, path, job);
      blockPointer = true;
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    }
    
    HiveIndexRecordReader indexReader = new HiveIndexRecordReader(inputFormat
        .getRecordReader(inputSplit, job, reporter));

    if (blockPointer) {
      indexReader.setBlockPointer(true);
      indexReader.setBlockStart(blockStart);
    }

    return indexReader;
  }

}
