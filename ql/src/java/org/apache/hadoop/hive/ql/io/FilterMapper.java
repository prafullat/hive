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
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * This is the mapper class for filtering the hive stream output for the given predicates.
 * Conf Input: hive.index.pred = comma separated list of value to search for
 *             hive.index.pred_pos = position of the predicate column in the input list (0 based)
 *             TODO this should come from execution plan but for now this is input by the client program
 * Map Input: output from HiveStreaming's select clause
 * Output: input rows for which output matches
 */
public class FilterMapper extends MapReduceBase
implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {

  private ArrayList<String> predicateValList = null;
  private int predPos;
  private boolean pr = true;
  public static final Log l4j = LogFactory.getLog("FilterMapper");


  @Override
  public void configure(JobConf job) {
    
    String predClause = job.get("hive.index.pred");
    String[] predList = predClause.split(",");
    if (predList.length < 1) {
      throw new RuntimeException("Configure: predicate clause should have ip addresses seperated by a comma");
    }

    predicateValList = new ArrayList<String> (predList.length);
    for (String pred : predList) {
      predicateValList.add(pred);
    }
    for (String string : predList) {
      l4j.info(string);
    }
    String predPosStr = job.get("hive.index.pred_pos");
    predPos = Integer.parseInt(predPosStr);
  }

  public void map(WritableComparable key, Writable value,
      OutputCollector output, Reporter reporter) throws IOException {
    // key and value (ip and user should be non-null
    if((key == null) || (value == null)) {
      return;
    }
    String [] cols = ((Text)value).toString().split("\t");
    if (cols.length < predPos) {
      if (pr) {
        pr = false;
        // there are not enough columns so just ignore this row
        l4j.info("Number of columns: " + cols.length + " Predicate pos: " + predPos);
        for (String string : cols) {
          l4j.info(string);
        }
      }
      return;
    }
    if(predPos == 0) {
      String indexedKeyCol = ((Text)key).toString();
      if(predicateValList.indexOf(indexedKeyCol) == -1) {
        // current key is not in the predicate val list so nothing do to for this row
        return;
      }
    } else {
      if(predicateValList.indexOf(cols[predPos-1]) == -1) {
        return;
      }
    }
    
//    String viewTime = ((Text)key).toString();
//    dt.setTime(Long.parseLong(viewTime) * 1000);
//    outKey.set(sdf.format(dt));
    
    // if it passes the equality predicate then just output it
//    output.collect(outKey, value);
    output.collect(key, value);
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
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * This is the mapper class for filtering the hive stream output for the given predicates.
 * Conf Input: hive.index.pred = comma separated list of value to search for
 *             hive.index.pred_pos = position of the predicate column in the input list (0 based)
 *             TODO this should come from execution plan but for now this is input by the client program
 * Map Input: output from HiveStreaming's select clause
 * Output: input rows for which output matches
 */
public class FilterMapper extends MapReduceBase
implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {

  private ArrayList<String> predicateValList = null;
  private int predPos;
  private boolean pr = true;
  public static final Log l4j = LogFactory.getLog("FilterMapper");


  @Override
  public void configure(JobConf job) {
    
    String predClause = job.get("hive.index.pred");
    String[] predList = predClause.split(",");
    if (predList.length < 1) {
      throw new RuntimeException("Configure: predicate clause should have ip addresses seperated by a comma");
    }

    predicateValList = new ArrayList<String> (predList.length);
    for (String pred : predList) {
      predicateValList.add(pred);
    }
    for (String string : predList) {
      l4j.info(string);
    }
    String predPosStr = job.get("hive.index.pred_pos");
    predPos = Integer.parseInt(predPosStr);
  }

  public void map(WritableComparable key, Writable value,
      OutputCollector output, Reporter reporter) throws IOException {
    // key and value (ip and user should be non-null
    if((key == null) || (value == null)) {
      return;
    }
    String [] cols = ((Text)value).toString().split("\t");
    if (cols.length < predPos) {
      if (pr) {
        pr = false;
        // there are not enough columns so just ignore this row
        l4j.info("Number of columns: " + cols.length + " Predicate pos: " + predPos);
        for (String string : cols) {
          l4j.info(string);
        }
      }
      return;
    }
    if(predPos == 0) {
      String indexedKeyCol = ((Text)key).toString();
      if(predicateValList.indexOf(indexedKeyCol) == -1) {
        // current key is not in the predicate val list so nothing do to for this row
        return;
      }
    } else {
      if(predicateValList.indexOf(cols[predPos-1]) == -1) {
        return;
      }
    }
    
//    String viewTime = ((Text)key).toString();
//    dt.setTime(Long.parseLong(viewTime) * 1000);
//    outKey.set(sdf.format(dt));
    
    // if it passes the equality predicate then just output it
//    output.collect(outKey, value);
    output.collect(key, value);
  }
}
