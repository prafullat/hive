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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public abstract class IndexBuilderBaseReducer extends MapReduceBase implements
    Reducer<WritableComparable, Writable, WritableComparable, Writable> {

  Serializer serializer;
  Deserializer deserializer;
  Class<? extends Writable> outputClass;
  RecordWriter outWriter;
  
  Path finalPath;
  FileSystem fs;
  
  String[] indexColumns;
  StructObjectInspector rowObjectInspector;
  StructObjectInspector keyObjectInspector;
  StandardStructObjectInspector indexRowOutputObjInspector;
  List<StructField> structFields = new ArrayList<StructField>();
  List<ObjectInspector> indexColsInspectors;
  
  boolean exception = false;
  boolean autoDelete = false;
  Path outPath;
  
  Object[] indexOutputObjects;

  public void configure(JobConf job) {
    MapredWork conf = Utilities.getMapRedWork(job);
    TableDesc table = conf.getIndexTableDesc();
    try {
      serializer = (Serializer) table.getDeserializerClass().newInstance();
      serializer.initialize(job, table.getProperties());
      outputClass = serializer.getSerializedClass();

      indexColumns = conf.getIndexCols().split(",");
      indexOutputObjects = new Object[indexColumns.length];
      indexColsInspectors = new ArrayList<ObjectInspector>();
      List<ObjectInspector> indexOutputRowInspectors = new ArrayList<ObjectInspector>();
      List<String> outputColName = new ArrayList<String>();
      List<String> indexColNames = new ArrayList<String>();
      deserializer = (Deserializer) HiveIndex.INDEX_MAPRED_BOUN_SERDE
          .newInstance();
      deserializer.initialize(job, table.getProperties());
      rowObjectInspector = (StructObjectInspector) deserializer
          .getObjectInspector();
      for (int k = 0; k < indexColumns.length - 2; k++) {
        String col = indexColumns[k];
        StructField field = rowObjectInspector.getStructFieldRef(col);
        structFields.add(field);
        ObjectInspector inspector = field.getFieldObjectInspector();
        if (!inspector.getCategory().equals(Category.PRIMITIVE)) {
          throw new RuntimeException("Only primitive columns can be indexed.");
        }
        outputColName.add(col);
        indexColNames.add(col);
        indexColsInspectors.add(inspector);
        indexOutputRowInspectors.add(ObjectInspectorUtils
            .getStandardObjectInspector(inspector, ObjectInspectorCopyOption.JAVA));
      }
      
      for (int k = indexColumns.length - 2; k < indexColumns.length; k++) {
        String col = indexColumns[k];
        StructField field = rowObjectInspector.getStructFieldRef(col);
        structFields.add(field);
        ObjectInspector inspector = field.getFieldObjectInspector();
        outputColName.add(col);
        indexOutputRowInspectors.add(ObjectInspectorUtils
            .getStandardObjectInspector(inspector, ObjectInspectorCopyOption.JAVA));
      }
      
      keyObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(indexColNames,
              indexColsInspectors);
      indexRowOutputObjInspector = ObjectInspectorFactory
      .getStandardStructObjectInspector(outputColName,
          indexOutputRowInspectors);
      
      boolean isCompressed = conf.getCompressed(); 
      String codecStr = conf.getCompressCodec();
      //the job's final output path
      String specPath = conf.getOutputPath();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId = Utilities.getTaskId(job);
      finalPath = new Path(tmpPath, taskId);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));
      fs = (new Path(specPath)).getFileSystem(job);
      outWriter = HiveFileFormatUtils.getHiveRecordWriter(job, table,
          outputClass, isCompressed, codecStr, "", outPath);
      HiveOutputFormat<?, ?> hiveOutputFormat = table
      .getOutputFileFormatClass().newInstance();
      
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(tmpPath, taskId,
          job, hiveOutputFormat, isCompressed, finalPath);
      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs, outPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public void reduce(WritableComparable key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {
    try {
      Object row = deserializer.deserialize((Writable) key);
      for (int i = 0; i < indexColumns.length - 2; i++) {
        this.indexOutputObjects[i] = ObjectInspectorUtils.copyToStandardObject(
            rowObjectInspector.getStructFieldData(row, structFields.get(i)),
            indexColsInspectors.get(i), ObjectInspectorCopyOption.JAVA);
      }
      doReduce(this.indexOutputObjects, values, reporter);
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }
  
  public abstract void doReduce(Object[] keys, Iterator values,
      Reporter reporter) throws IOException, SerDeException;
  
  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter != null) {
      outWriter.close(exception);
      outWriter = null;
    }
    
    if (!exception) {
      FileStatus fss = fs.getFileStatus(outPath);
      System.out.println("renamed path " + outPath + " to " + finalPath
          + " . File size is " + fss.getLen());
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to " + finalPath);
      }
    } else {
      if(!autoDelete) {
        fs.delete(outPath, true);  
      }
    }
  }
  
  public static void indexBuilderJobClose(String outputPath, boolean success,
      JobConf job, LogHelper console) throws HiveException, IOException {
    FileSystem fs =  (new Path(outputPath)).getFileSystem(job);
    Path tmpPath = Utilities.toTempPath(outputPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName() + ".intemediate");
    Path finalPath = new Path(outputPath);
    System.out.println("tmpPath is " + tmpPath);
    System.out.println("intemediatePath is " + intermediatePath);
    System.out.println("finalPath is " + finalPath);
    if (success) {
      if (fs.exists(tmpPath)) {
        
        if(fs.exists(intermediatePath)) {
          fs.delete(intermediatePath, true);
        }
        fs.mkdirs(intermediatePath);
        FileStatus[] fss = fs.listStatus(tmpPath);
        for (FileStatus f : fss) {
          fs.rename(f.getPath(), new Path(intermediatePath, f.getPath()
              .getName()));
        }
        
        fss = fs.listStatus(intermediatePath);
        long len = 0;
        for (FileStatus f : fss) {
          len += f.getLen();
        }
        console.printInfo("IntermediatePath Path's file number is " + fss.length + ", total size is "
            + len);
        
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        
        fss = fs.listStatus(finalPath);
        for (FileStatus f : fss) {
          fs.delete(f.getPath(), true);
        }
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
        fs.delete(tmpPath, true);
        fs.delete(intermediatePath, true);
        
        fss = fs.listStatus(finalPath);
        len = 0;
        for (FileStatus f : fss) {
          len += f.getLen();
        }
        console.printInfo("Final Path's file number is " + fss.length + ", total size is "
            + len);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }

}
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public abstract class IndexBuilderBaseReducer extends MapReduceBase implements
    Reducer<WritableComparable, Writable, WritableComparable, Writable> {

  Serializer serializer;
  Deserializer deserializer;
  Class<? extends Writable> outputClass;
  RecordWriter outWriter;
  
  Path finalPath;
  FileSystem fs;
  
  String[] indexColumns;
  StructObjectInspector rowObjectInspector;
  StructObjectInspector keyObjectInspector;
  StandardStructObjectInspector indexRowOutputObjInspector;
  List<StructField> structFields = new ArrayList<StructField>();
  List<ObjectInspector> indexColsInspectors;
  
  boolean exception = false;
  boolean autoDelete = false;
  Path outPath;
  
  Object[] indexOutputObjects;

  public void configure(JobConf job) {
    MapredWork conf = Utilities.getMapRedWork(job);
    TableDesc table = conf.getIndexTableDesc();
    try {
      serializer = (Serializer) table.getDeserializerClass().newInstance();
      serializer.initialize(job, table.getProperties());
      outputClass = serializer.getSerializedClass();

      indexColumns = conf.getIndexCols().split(",");
      indexOutputObjects = new Object[indexColumns.length];
      indexColsInspectors = new ArrayList<ObjectInspector>();
      List<ObjectInspector> indexOutputRowInspectors = new ArrayList<ObjectInspector>();
      List<String> outputColName = new ArrayList<String>();
      List<String> indexColNames = new ArrayList<String>();
      deserializer = (Deserializer) HiveIndex.INDEX_MAPRED_BOUN_SERDE
          .newInstance();
      deserializer.initialize(job, table.getProperties());
      rowObjectInspector = (StructObjectInspector) deserializer
          .getObjectInspector();
      for (int k = 0; k < indexColumns.length - 2; k++) {
        String col = indexColumns[k];
        StructField field = rowObjectInspector.getStructFieldRef(col);
        structFields.add(field);
        ObjectInspector inspector = field.getFieldObjectInspector();
        if (!inspector.getCategory().equals(Category.PRIMITIVE)) {
          throw new RuntimeException("Only primitive columns can be indexed.");
        }
        outputColName.add(col);
        indexColNames.add(col);
        indexColsInspectors.add(inspector);
        indexOutputRowInspectors.add(ObjectInspectorUtils
            .getStandardObjectInspector(inspector, ObjectInspectorCopyOption.JAVA));
      }
      
      for (int k = indexColumns.length - 2; k < indexColumns.length; k++) {
        String col = indexColumns[k];
        StructField field = rowObjectInspector.getStructFieldRef(col);
        structFields.add(field);
        ObjectInspector inspector = field.getFieldObjectInspector();
        outputColName.add(col);
        indexOutputRowInspectors.add(ObjectInspectorUtils
            .getStandardObjectInspector(inspector, ObjectInspectorCopyOption.JAVA));
      }
      
      keyObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(indexColNames,
              indexColsInspectors);
      indexRowOutputObjInspector = ObjectInspectorFactory
      .getStandardStructObjectInspector(outputColName,
          indexOutputRowInspectors);
      
      boolean isCompressed = conf.getCompressed(); 
      String codecStr = conf.getCompressCodec();
      //the job's final output path
      String specPath = conf.getOutputPath();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId = Utilities.getTaskId(job);
      finalPath = new Path(tmpPath, taskId);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));
      fs = (new Path(specPath)).getFileSystem(job);
      outWriter = HiveFileFormatUtils.getHiveRecordWriter(job, table,
          outputClass, isCompressed, codecStr, "", outPath);
      HiveOutputFormat<?, ?> hiveOutputFormat = table
      .getOutputFileFormatClass().newInstance();
      
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(tmpPath, taskId,
          job, hiveOutputFormat, isCompressed, finalPath);
      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs, outPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public void reduce(WritableComparable key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {
    try {
      Object row = deserializer.deserialize((Writable) key);
      for (int i = 0; i < indexColumns.length - 2; i++) {
        this.indexOutputObjects[i] = ObjectInspectorUtils.copyToStandardObject(
            rowObjectInspector.getStructFieldData(row, structFields.get(i)),
            indexColsInspectors.get(i), ObjectInspectorCopyOption.JAVA);
      }
      doReduce(this.indexOutputObjects, values, reporter);
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }
  
  public abstract void doReduce(Object[] keys, Iterator values,
      Reporter reporter) throws IOException, SerDeException;
  
  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter != null) {
      outWriter.close(exception);
      outWriter = null;
    }
    
    if (!exception) {
      FileStatus fss = fs.getFileStatus(outPath);
      System.out.println("renamed path " + outPath + " to " + finalPath
          + " . File size is " + fss.getLen());
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to " + finalPath);
      }
    } else {
      if(!autoDelete) {
        fs.delete(outPath, true);  
      }
    }
  }
  
  public static void indexBuilderJobClose(String outputPath, boolean success,
      JobConf job, LogHelper console) throws HiveException, IOException {
    FileSystem fs =  (new Path(outputPath)).getFileSystem(job);
    Path tmpPath = Utilities.toTempPath(outputPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName() + ".intemediate");
    Path finalPath = new Path(outputPath);
    System.out.println("tmpPath is " + tmpPath);
    System.out.println("intemediatePath is " + intermediatePath);
    System.out.println("finalPath is " + finalPath);
    if (success) {
      if (fs.exists(tmpPath)) {
        
        if(fs.exists(intermediatePath)) {
          fs.delete(intermediatePath, true);
        }
        fs.mkdirs(intermediatePath);
        FileStatus[] fss = fs.listStatus(tmpPath);
        for (FileStatus f : fss) {
          fs.rename(f.getPath(), new Path(intermediatePath, f.getPath()
              .getName()));
        }
        
        fss = fs.listStatus(intermediatePath);
        long len = 0;
        for (FileStatus f : fss) {
          len += f.getLen();
        }
        console.printInfo("IntermediatePath Path's file number is " + fss.length + ", total size is "
            + len);
        
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        
        fss = fs.listStatus(finalPath);
        for (FileStatus f : fss) {
          fs.delete(f.getPath(), true);
        }
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
        fs.delete(tmpPath, true);
        fs.delete(intermediatePath, true);
        
        fss = fs.listStatus(finalPath);
        len = 0;
        for (FileStatus f : fss) {
          len += f.getLen();
        }
        console.printInfo("Final Path's file number is " + fss.length + ", total size is "
            + len);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }

}
