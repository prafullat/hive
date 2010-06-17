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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

public abstract class IndexBuilderBaseMapper extends MapReduceBase implements Mapper {

  private JobConf jc;
  private MapredWork conf;

  private Deserializer deserializer;
  private StructObjectInspector rowObjectInspector;

  public final Log LOG = LogFactory.getLog("IndexBuilderBaseMapper");

  private String bucketName;
  String[] indexColumns;

  private List<StructField> structFields = new ArrayList<StructField>();

  protected HiveKey cachedIndexCols = new HiveKey();

  private Object[] cachedKeys;

  protected IndexEntryValueCell cachedPos;

  transient Serializer keySerializer;
  transient StructObjectInspector keyObjectInspector;
  transient boolean keyIsText;

  public IndexBuilderBaseMapper() {
  }

  public void configure(JobConf job) {
    try {
      jc = job;

      conf = Utilities.getMapRedWork(job);
      bucketName = jc.get("map.input.file");
      TableDesc table = conf.getKeyDesc();

      Properties p = table.getProperties();
      Class<? extends Deserializer> deserializerClass = table
          .getDeserializerClass();
      if (deserializerClass == null) {
        String className = table.getSerdeClassName();
        if ((className == "") || (className == null)) {
          throw new HiveException(
              "SerDe class or the SerDe class name is not set for table: "
                  + table.getProperties().getProperty("name"));
        }
        deserializerClass = (Class<? extends Deserializer>) getClass()
            .getClassLoader().loadClass(className);
      }

      deserializer = (Deserializer) deserializerClass.newInstance();
      deserializer.initialize(jc, p);
      rowObjectInspector = (StructObjectInspector) deserializer
          .getObjectInspector();
      indexColumns = conf.getIndexCols().split(",");
      
      List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
      List<String> outputColName = new ArrayList<String>();
      for (int k = 0; k < indexColumns.length -2 ; k++) {
        String col = indexColumns[k];
        StructField field = rowObjectInspector.getStructFieldRef(col);
        structFields.add(field);
        ObjectInspector inspector = field.getFieldObjectInspector();
        if (!inspector.getCategory().equals(Category.PRIMITIVE)) {
          throw new RuntimeException("Only primitive columns can be indexed.");
        }
        outputColName.add(col);
        fieldObjectInspectors.add(inspector);
      }
      
      keyObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(outputColName,
              fieldObjectInspectors);

      TableDesc keyTableDesc = conf.getIndexTableDesc();
      keySerializer = (Serializer) HiveIndex.INDEX_MAPRED_BOUN_SERDE.newInstance();
      keySerializer.initialize(jc, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      cachedKeys = new Object[indexColumns.length - 2];
      cachedPos = (IndexEntryValueCell) WritableFactories.newInstance(IndexEntryValueCell.class);
      cachedPos.setBucketName(bucketName);

    } catch (Exception e) {
      LOG.error("Error in building index: " + e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected void perpareMapper(Object key, Object value) throws SerDeException {
    Object row = deserializer.deserialize((Writable) value);
    for (int i = 0; i < indexColumns.length -2; i++) {
      cachedKeys[i] = rowObjectInspector.getStructFieldData(row, structFields
          .get(i));
    }

    if (keyIsText) {
      Text k = (Text) keySerializer.serialize(cachedKeys, keyObjectInspector);
      cachedIndexCols.set(k.getBytes(), 0, k.getLength());
    } else {
      // Must be BytesWritable
      BytesWritable k = (BytesWritable) keySerializer.serialize(cachedKeys,
          keyObjectInspector);
      cachedIndexCols.set(k.get(), 0, k.getSize());
    }

    int keyHashCode = 0;
    for (int i = 0; i < cachedKeys.length; i++) {
      Object o = cachedKeys[i];
      keyHashCode = keyHashCode * 31
          + ObjectInspectorUtils.hashCode(o, structFields.get(i).getFieldObjectInspector());
    }
    cachedIndexCols.setHashCode(keyHashCode);
    
    cachedPos.setPosition(((LongWritable) key).get());
  }
}
