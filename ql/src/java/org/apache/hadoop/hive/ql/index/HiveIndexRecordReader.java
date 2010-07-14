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

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

public class HiveIndexRecordReader implements IndexRecordReader {

  private RecordReader rawReader;

  private LongWritable cachedKey = new LongWritable();

  private Object rawKey;
  
  private boolean blockPointer = false;
  private long blockStart = -1;
  
  private long currentBlockStart = 0;
  private long nextBlockStart = -1;

  public HiveIndexRecordReader(RecordReader recordReader) throws IOException {
    this.rawReader = recordReader;
    rawKey = recordReader.createKey();
    this.currentBlockStart = this.rawReader.getPos();
  }

  public void close() throws IOException {
    rawReader.close();
  }

  public LongWritable createKey() {
    return cachedKey;
  }

  public Object createValue() {
    return rawReader.createValue();
  }

  public long getPos() throws IOException {
    return rawReader.getPos();
  }

  public float getProgress() throws IOException {
    return rawReader.getProgress();
  }

  /**
   * the key object from raw record reader is throw away here, and is not passed
   * to upper.
   */
  public boolean next(Object key, Object value) throws IOException {
    boolean result = rawReader.next(rawKey, value);
    ((LongWritable) key).set(this.getCurrentValIndexOffset());
    return result;
  }

  public void setBlockPointer(boolean b) {
    blockPointer = b;
  }

  public void setBlockStart(long blockStart) {
    this.blockStart = blockStart;
  }

  @Override
  public long getCurrentValIndexOffset() throws IOException {
    if(this.rawReader instanceof IndexRecordReader) {
      return ((IndexRecordReader)this.rawReader).getCurrentValIndexOffset();
    }
    
    long ret = this.currentBlockStart;
    long pointerPos = this.rawReader.getPos();
    if(this.nextBlockStart == -1) {
      this.nextBlockStart = pointerPos;
      return ret;
    }
    
    if (pointerPos != this.nextBlockStart) {
      // the reader pointer has moved to the end of next block, or the end of
      // next record. (actually, here's next is current, because we already did
      // the read action.)
      
      this.currentBlockStart = this.nextBlockStart;
      this.nextBlockStart = pointerPos;
      if(blockPointer) {
        // we need the beginning of the current block.
        ret = this.currentBlockStart;        
      }
    }
   
    return ret;
  }
}
