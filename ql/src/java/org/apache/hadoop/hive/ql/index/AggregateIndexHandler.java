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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.compact.IndexMetadataChangeTask;
import org.apache.hadoop.hive.ql.index.compact.IndexMetadataChangeWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;


public class AggregateIndexHandler extends TableBasedIndexHandler {

    @Override
    public void analyzeIndexDefinition(Table baseTable, Index index,
        Table indexTable) throws HiveException {
      StorageDescriptor storageDesc = index.getSd();
      if (this.usesIndexTable() && indexTable != null) {
        StorageDescriptor indexTableSd = storageDesc.deepCopy();
        List<FieldSchema> indexTblCols = indexTableSd.getCols();
        FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
        indexTblCols.add(bucketFileName);
        FieldSchema offSets = new FieldSchema("_offsets", "array<bigint>", "");
        indexTblCols.add(offSets);
        FieldSchema countkey = new FieldSchema("_countkey", "int", "");
        indexTblCols.add(countkey);
        FieldSchema countall = new FieldSchema("_countall", "int", "");
        indexTblCols.add(countall);
        indexTable.setSd(indexTableSd);
      }
    }


    @Override
    protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
        List<FieldSchema> indexField, boolean partitioned,
        PartitionDesc indexTblPartDesc, String indexTableName,
        PartitionDesc baseTablePartDesc, String baseTableName, String dbName) {

      String indexCols = HiveUtils.getUnparsedColumnNamesFromFieldSchema(indexField);

      //form a new insert overwrite query.
      StringBuilder command= new StringBuilder();
      LinkedHashMap<String, String> partSpec = indexTblPartDesc.getPartSpec();

      command.append("INSERT OVERWRITE TABLE " + HiveUtils.unparseIdentifier(indexTableName ));
      if (partitioned && indexTblPartDesc != null) {
        command.append(" PARTITION ( ");
        List<String> ret = getPartKVPairStringArray(partSpec);
        for (int i = 0; i < ret.size(); i++) {
          String partKV = ret.get(i);
          command.append(partKV);
          if (i < ret.size() - 1) {
            command.append(",");
          }
        }
        command.append(" ) ");
      }

      command.append(" SELECT ");
      command.append(indexCols);
      command.append(",");

      command.append(VirtualColumn.FILENAME.getName());
      command.append(",");
      command.append(" collect_set (");
      command.append(VirtualColumn.BLOCKOFFSET.getName());
      command.append(") ");
      command.append(",");
      if(!indexCols.contains(",")){
        command.append(" count(");
        command.append(indexCols);
        command.append(") ");
        command.append(",");
      }else{
        String[] colList = indexCols.split(",");
        for (int i = 0; i < colList.length; i++) {
          command.append(" count(");
          command.append(colList[i]);
          command.append(") ");
          command.append(",");
        }
      }
      command.append(" count(*) ");
      command.append(" FROM " + HiveUtils.unparseIdentifier(baseTableName) );
      LinkedHashMap<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
      if(basePartSpec != null) {
        command.append(" WHERE ");
        List<String> pkv = getPartKVPairStringArray(basePartSpec);
        for (int i = 0; i < pkv.size(); i++) {
          String partKV = pkv.get(i);
          command.append(partKV);
          if (i < pkv.size() - 1) {
            command.append(" AND ");
          }
        }
      }
      command.append(" GROUP BY ");
      command.append(indexCols + ", " + VirtualColumn.FILENAME.getName());

      Driver driver = new Driver(new HiveConf(getConf(), AggregateIndexHandler.class));
      driver.compile(command.toString());

      Task<?> rootTask = driver.getPlan().getRootTasks().get(0);
      inputs.addAll(driver.getPlan().getInputs());
      outputs.addAll(driver.getPlan().getOutputs());

      IndexMetadataChangeWork indexMetaChange = new IndexMetadataChangeWork(partSpec, indexTableName, dbName);
      IndexMetadataChangeTask indexMetaChangeTsk = new IndexMetadataChangeTask();
      indexMetaChangeTsk.setWork(indexMetaChange);
      rootTask.addDependentTask(indexMetaChangeTsk);

      return rootTask;
    }
  }