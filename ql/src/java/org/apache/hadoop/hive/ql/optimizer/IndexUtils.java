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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeTask;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeWork;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.index.IndexWhereProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Utility class for index support.
 * Currently used for BITMAP and AGGREGATE index
 *
 */
public final class IndexUtils {

  private static final Log LOG = LogFactory.getLog(IndexWhereProcessor.class.getName());

  private IndexUtils(){
  }

  /**
   * Check the partitions used by the table scan to make sure they also exist in the
   * index table.
   * @param pctx
   * @param indexes
   * @return partitions used by query.  null if they do not exist in index table
   * @throws HiveException
   */
  public static Set<Partition> checkPartitionsCoveredByIndex(TableScanOperator tableScan,
      ParseContext pctx,
      Map<Table, List<Index>> indexes)
    throws HiveException {
    Hive hive = Hive.get(pctx.getConf());
    Set<Partition> queryPartitions = null;
    // make sure each partition exists on the index table
    PrunedPartitionList queryPartitionList = pctx.getOpToPartList().get(tableScan);
    if(queryPartitionList.getConfirmedPartns() != null
        && !queryPartitionList.getConfirmedPartns().isEmpty()){
      queryPartitions = queryPartitionList.getConfirmedPartns();
    }else if(queryPartitionList.getUnknownPartns() != null
        && !queryPartitionList.getUnknownPartns().isEmpty()){
      queryPartitions = queryPartitionList.getUnknownPartns();
    }

    for (Partition part : queryPartitions) {
      List<Table> sourceIndexTables = getIndexTables(hive, part, indexes);
      if (!containsPartition(hive, sourceIndexTables, part)) {
        return null; // problem if it doesn't contain the partition
      }
    }

    return queryPartitions;
  }

  /**
   * return index tables associated with the base table of the partition.
   */
  private static List<Table> getIndexTables(Hive hive, Partition part,
      Map<Table, List<Index>> indexes) throws HiveException {
    List<Table> indexTables = new ArrayList<Table>();
    Table partitionedTable = part.getTable();
    for (Index index : indexes.get(partitionedTable)) {
      indexTables.add(hive.getTable(index.getIndexTableName()));
    }
    return indexTables;
  }

  /**
   * check that every index table contains the given partition.
   */
  private static boolean containsPartition(Hive hive, List<Table> indexTables, Partition part)
    throws HiveException {
    Map<String, String> partSpec = part.getSpec();

    if (partSpec.isEmpty()) {
      return true; // empty specs come from non-partitioned tables
    }

    for (Table indexTable : indexTables) {
      // get partitions that match the spec
      List<Partition> matchingPartitions = hive.getPartitions(indexTable, partSpec);
      if (matchingPartitions == null || matchingPartitions.size() == 0) {
        LOG.info("Index table " + indexTable +
            "did not contain built partition that matched " + partSpec);
        return false;
      }
    }
    return true;
  }

  /**
   * Get a list of indexes on a table that match given types.
   */
  public static List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes)
    throws SemanticException {
    List<Index> matchingIndexes = new ArrayList<Index>();
    List<Index> indexesOnTable = null;

    try {
      indexesOnTable = baseTableMetaData.getAllIndexes((short) -1); // get all indexes
    } catch (HiveException e) {
      throw new SemanticException("Error accessing metastore", e);
    }

    for (Index index : indexesOnTable) {
      String indexType = index.getIndexHandlerClass();
      if (matchIndexTypes.contains(indexType)) {
        matchingIndexes.add(index);
      }
    }
    return matchingIndexes;
  }

  public static Task<?> createRootTask(HiveConf builderConf, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, StringBuilder command,
      LinkedHashMap<String, String> partSpec,
      String indexTableName, String dbName){
    // Don't try to index optimize the query to build the index
    HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
    Driver driver = new Driver(builderConf);
    driver.compile(command.toString());

    Task<?> rootTask = driver.getPlan().getRootTasks().get(0);
    inputs.addAll(driver.getPlan().getInputs());
    outputs.addAll(driver.getPlan().getOutputs());

    IndexMetadataChangeWork indexMetaChange = new IndexMetadataChangeWork(partSpec,
        indexTableName, dbName);
    IndexMetadataChangeTask indexMetaChangeTsk = new IndexMetadataChangeTask();
    indexMetaChangeTsk.setWork(indexMetaChange);
    rootTask.addDependentTask(indexMetaChangeTsk);

    return rootTask;
  }


}
