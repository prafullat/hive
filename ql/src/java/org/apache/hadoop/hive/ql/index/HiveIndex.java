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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Holds index related constants
 * Actual logic is spread in HiveStreamingRecordReder and IndexReducer classes.
 * 
 */
public class HiveIndex {

  public static final Log l4j = LogFactory.getLog("HiveIndex");
  
  public static final Class INDEX_MAPRED_BOUN_SERDE = LazySimpleSerDe.class;
  public static final String IDX_BUCKET_COL_NAME = "_bucketname";
  public static final String IDX_OFFSET_COL_NAME = "_offsets";
  
  public static enum IndexType {
    COMPACT_SUMMARY_TABLE("compact"),
    SUMMARY_TABLE("summary"),
    PROJECTION("projection");

    private IndexType(String indexType) {
      indexTypeName = indexType;
    }

    private String indexTypeName;

    public String getName() {
      return indexTypeName;
    }
  }
  
  public static IndexType getIndexType(String name) {
    IndexType[] types = IndexType.values();
    for (IndexType type : types) {
      if(type.getName().equals(name.toLowerCase()))
        return type;
    }
    throw new IllegalArgumentException(name + " is not a valid index type.");
  }
  
  
  // modeled on sequence file record reader
  public static class IndexSequenceFileRecordReader<K extends WritableComparable, V extends Writable>
      implements RecordReader<K, V> {

    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean more = true;
    protected Configuration conf;

    public IndexSequenceFileRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      Path path = split.getPath();
      FileSystem fs = path.getFileSystem(conf);
      this.in = new SequenceFile.Reader(fs, path, conf);
      this.end = split.getStart() + split.getLength();
      this.conf = conf;

      if (split.getStart() > in.getPosition())
        in.sync(split.getStart()); // sync to start

      this.start = in.getPosition();
      more = start < end;
    }

    public void sync(long offset) throws IOException {
      if(offset > end) {
        offset = end;
      }
      in.sync(offset);
      this.start = in.getPosition();
      more = start < end;
    }

    /**
     * The class of key that must be passed to {@link
     * #next(WritableComparable,Writable)}..
     */
    public Class getKeyClass() {
      return in.getKeyClass();
    }

    /**
     * The class of value that must be passed to {@link
     * #next(WritableComparable,Writable)}..
     */
    public Class getValueClass() {
      return in.getValueClass();
    }

    @SuppressWarnings("unchecked")
    public K createKey() {
      return (K) ReflectionUtils.newInstance(getKeyClass(), conf);
    }

    @SuppressWarnings("unchecked")
    public V createValue() {
      return (V) ReflectionUtils.newInstance(getValueClass(), conf);
    }

    public synchronized boolean next(K key, V value) throws IOException {
      if (!more)
        return false;
      long pos = in.getPosition();
      boolean eof = in.next(key, value);
      if (pos >= end && in.syncSeen()) {
        more = false;
      } else {
        more = eof;
      }
      return more;
    }

    public synchronized boolean next(K key) throws IOException {
      if (!more)
        return false;
      long pos = in.getPosition();
      boolean eof = in.next(key);
      if (pos >= end && in.syncSeen()) {
        more = false;
      } else {
        more = eof;
      }
      return more;
    }

    public synchronized void getCurrentValue(V value) throws IOException {
      in.getCurrentValue(value);
    }

    /**
     * Return the progress within the input split
     * 
     * @return 0.0 to 1.0 of the input byte range
     */
    public float getProgress() throws IOException {
      if (end == start) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (in.getPosition() - start)
            / (float) (end - start));
      }
    }

    public synchronized long getPos() throws IOException {
      return in.getPosition();
    }

    public synchronized void seek(long pos) throws IOException {
      in.seek(pos);
    }

    public synchronized void close() throws IOException {
      in.close();
    }

    public boolean syncSeen() {
      return in.syncSeen();
    }
  }
  
  //IndexBucket
  public static class IBucket {
    private String name = null;
    private SortedSet<Long> offsets = new TreeSet<Long>();
    public IBucket(String n) {
      name = n;
    }
    public void add(Long offset) {
      offsets.add(offset);
    }
    public String getName() {
      return name;
    }
    public SortedSet<Long> getOffsets() {
      return offsets;
    }
    public boolean equals(Object obj) {
      if(obj.getClass() != this.getClass()) {
        return false;
      }
      return (((IBucket)obj).name.compareToIgnoreCase(this.name) == 0);
    }
  }
  
  public static class HiveIndexResult {
    private HashMap<String, TreeMap<Date, Vector<IBucket>>> indexResult = new HashMap<String, TreeMap<Date, Vector<IBucket>>>();
    JobConf job = null;
    
    BytesRefWritable[] bytesRef = new BytesRefWritable[2];
    
    public HiveIndexResult(String indexFile, JobConf conf) throws IOException, HiveException {
      job = conf;
      
      bytesRef[0] = new BytesRefWritable();
      bytesRef[1] = new BytesRefWritable();
      
      if(indexFile != null) {
        Path indexFilePath = new Path(indexFile);
        FileSystem fs = FileSystem.get(conf);
        FileStatus indexStat = fs.getFileStatus(indexFilePath);
        List<Path> paths = new ArrayList<Path>();
        if(indexStat.isDir()) {
          FileStatus[] fss = fs.listStatus(indexFilePath);
          for (FileStatus f : fss) {
            paths.add(f.getPath());
          }
        } else {
          paths.add(indexFilePath);
        }
        
        for(Path indexFinalPath : paths) {
          FSDataInputStream ifile = fs.open(indexFinalPath);
          LineReader lr = new LineReader(ifile, conf);
          Text line = new Text();
          while( lr.readLine(line) > 0) {
            add(line);
          }
          // this will close the input stream
          lr.close();
        }
      }
    }


    Map<String, IBucket> buckets = new HashMap<String, IBucket>();

    private void add(Text line) throws HiveException {
      String l = line.toString();
      byte[] bytes = l.getBytes();
      int firstEnd = 0;
      int i = 0;
      for (int index = 0; index < bytes.length; index++) {
        if (bytes[index] == LazySimpleSerDe.DefaultSeparators[0]) {
          i++;
          firstEnd = index;
        }
      }
      if (i > 1) {
        throw new HiveException(
            "Bad index file row (index file should only contain two columns: bucket_file_name and offset lists.) ."
                + line.toString());
      }
      String bucketFileName = new String(bytes, 0, firstEnd);
      IBucket bucket = buckets.get(bucketFileName);
      if(bucket == null) {
        bucket = new IBucket(bucketFileName);
        buckets.put(bucketFileName, bucket);
      }
      
      int currentStart = firstEnd + 1;
      int currentEnd = firstEnd + 1;
      for (; currentEnd < bytes.length; currentEnd++) {
        if (bytes[currentEnd] == LazySimpleSerDe.DefaultSeparators[1]) {
          String one_offset = new String (bytes, currentStart, currentEnd - currentStart);
          Long offset = Long.parseLong(one_offset);
          bucket.getOffsets().add(offset);
          currentStart = currentEnd + 1;
        }
      }
      String one_offset = new String(bytes, currentStart, currentEnd
          - currentStart);
      bucket.getOffsets().add(Long.parseLong(one_offset));
    }
    

    public boolean contains(FileSplit split) throws HiveException {
      
      if(buckets == null) {
        return false;
      }
      String bucketName = split.getPath().toString();
      IBucket bucket = buckets.get(bucketName);
      if(bucket == null) {
        bucketName = split.getPath().toUri().getPath();
        bucket = buckets.get(bucketName);
        if(bucket == null) {
          return false;          
        }
      }
      
      for (Long offset : bucket.getOffsets()) {
        if ( (offset >= split.getStart()) && (offset <= split.getStart() + split.getLength())) {
          return true;
        }
      }
      return false;
    }
  }
  
}
