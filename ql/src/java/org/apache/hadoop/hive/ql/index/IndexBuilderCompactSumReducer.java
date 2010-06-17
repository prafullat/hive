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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class is reducer for CreateIndex Hive command. Input: <<key1^Dkey2>,
 * <bucket^Coffset>+> Output: A Hive table of key, value separated by
 * ^A<key1^Dkey2
 * >^A<bucket^CList<offset>>^B<bucket^List<offset>>^B<bucket^List<offset>> so on
 * 
 */
public class IndexBuilderCompactSumReducer extends IndexBuilderBaseReducer {

  private Map<String, SortedSet<Long>> bucketOffsetMap = new HashMap<String, SortedSet<Long>>();
  
  private Writable outVal;

  //private NullWritable key = NullWritable.get();

  public IndexBuilderCompactSumReducer() {
  }

  public void doReduce(Object[] keys, Iterator values,
      Reporter reporter) throws IOException, SerDeException {
    reporter.progress();
    bucketOffsetMap.clear();
    while (values.hasNext()) {
      IndexEntryValueCell value = (IndexEntryValueCell) values.next();
      String bucketName = value.getBucketName();
      long offset = value.getPosition();
      SortedSet<Long> bucketPos = bucketOffsetMap.get(bucketName);
      if (bucketPos == null) {
        bucketPos = new TreeSet<Long>();
        bucketOffsetMap.put(bucketName, bucketPos);
      }
      if (!bucketPos.contains(offset))
        bucketPos.add(offset);
    }
    
    Iterator<String> it = bucketOffsetMap.keySet().iterator();
    List<String> offsets = new ArrayList<String>();
    while (it.hasNext()) {
      String bucketName = it.next();
      this.indexOutputObjects[this.indexColumns.length-2] = bucketName;
      SortedSet<Long> poses = bucketOffsetMap.get(bucketName);
      Iterator<Long> posIter = poses.iterator();
      offsets.clear();
      while (posIter.hasNext()) {
        offsets.add(posIter.next().toString());
      }
      this.indexOutputObjects[this.indexColumns.length-1] = offsets;
      outVal = this.serializer.serialize(this.indexOutputObjects, indexRowOutputObjInspector);
      this.outWriter.write(outVal);
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class is reducer for CreateIndex Hive command. Input: <<key1^Dkey2>,
 * <bucket^Coffset>+> Output: A Hive table of key, value separated by
 * ^A<key1^Dkey2
 * >^A<bucket^CList<offset>>^B<bucket^List<offset>>^B<bucket^List<offset>> so on
 * 
 */
public class IndexBuilderCompactSumReducer extends IndexBuilderBaseReducer {

  private Map<String, SortedSet<Long>> bucketOffsetMap = new HashMap<String, SortedSet<Long>>();
  
  private Writable outVal;

  //private NullWritable key = NullWritable.get();

  public IndexBuilderCompactSumReducer() {
  }

  public void doReduce(Object[] keys, Iterator values,
      Reporter reporter) throws IOException, SerDeException {
    reporter.progress();
    bucketOffsetMap.clear();
    while (values.hasNext()) {
      IndexEntryValueCell value = (IndexEntryValueCell) values.next();
      String bucketName = value.getBucketName();
      long offset = value.getPosition();
      SortedSet<Long> bucketPos = bucketOffsetMap.get(bucketName);
      if (bucketPos == null) {
        bucketPos = new TreeSet<Long>();
        bucketOffsetMap.put(bucketName, bucketPos);
      }
      if (!bucketPos.contains(offset))
        bucketPos.add(offset);
    }
    
    Iterator<String> it = bucketOffsetMap.keySet().iterator();
    List<String> offsets = new ArrayList<String>();
    while (it.hasNext()) {
      String bucketName = it.next();
      this.indexOutputObjects[this.indexColumns.length-2] = bucketName;
      SortedSet<Long> poses = bucketOffsetMap.get(bucketName);
      Iterator<Long> posIter = poses.iterator();
      offsets.clear();
      while (posIter.hasNext()) {
        offsets.add(posIter.next().toString());
      }
      this.indexOutputObjects[this.indexColumns.length-1] = offsets;
      outVal = this.serializer.serialize(this.indexOutputObjects, indexRowOutputObjInspector);
      this.outWriter.write(outVal);
    }
    
  }

}
