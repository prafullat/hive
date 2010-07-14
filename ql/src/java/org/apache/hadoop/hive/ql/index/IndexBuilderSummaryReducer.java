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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

public class IndexBuilderSummaryReducer extends IndexBuilderBaseReducer {

  private StringBuilder bl = null;
  private Text outVal = new Text();

  private NullWritable key = NullWritable.get();

  public IndexBuilderSummaryReducer() {
  }

  public void doReduce(Object[] keys, Iterator values, Reporter reporter) throws IOException {
//    reporter.progress();
//    while (values.hasNext()) {
//      bl = new StringBuilder();
//      // would toString() change the byte order?
//      String keyPart = Text.decode(((HiveKey) key).getBytes());
//      bl.append(keyPart);
//      bl.append(HiveIndex.KEY_VAL_LIST_SEPARATOR);
//      IndexEntryValueCell value = (IndexEntryValueCell) values.next();
//      String bucketName = value.getBucketName();
//      bl.append(bucketName);
//      bl.append(HiveIndex.BUCKET_POS_VAL_SEPARATOR);
//      bl.append(value.getPosition());
//      outVal.set(bl.toString());
//      this.outWriter.write(outVal);
//    }
  }
}
