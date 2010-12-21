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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  //vars
  int NO_OF_SUBQUERIES = 0;
  int NO_OF_TABLES = 0;
  boolean QUERY_HAS_JOIN = false;
  boolean TABLE_HAS_NO_INDEX = false;
  boolean QUERY_HAS_SORT_BY = false;
  boolean QUERY_HAS_DISTRIBUTE_BY = false;
  int AGG_FUNC_COUNT = 0;
  boolean AGG_FUNC_IS_NOT_COUNT = false;
  boolean FUNC_COUNT_COLS_FETCH_EXCEPTION = false;
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
  private String topTable = null;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    opToParseCtxMap = parseContext.getOpParseCtx();
    try {
      hiveDb = Hive.get(parseContext.getConf());
    } catch (HiveException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if( shouldApplyOptimization() == true ) {
      return parseContext;
    }

    return null;
  }

  public String getName() {
    return "RewriteGBUsingIndex";
  }


  private List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes) {
    List<Index> matchingIndexes = new ArrayList<Index>();
    List<Index> indexesOnTable = null;

    try {
      short maxNumOfIndexes = 1024; // XTODO: Hardcoding. Need to know if
      // there's a limit (and what is it) on
      // # of indexes that can be created
      // on a table. If not, why is this param
      // required by metastore APIs?
      indexesOnTable = baseTableMetaData.getAllIndexes(maxNumOfIndexes);

    } catch (HiveException e) {
      return matchingIndexes; // Return empty list (trouble doing rewrite
      // shouldn't stop regular query execution,
      // if there's serious problem with metadata
      // or anything else, it's assumed to be
      // checked & handled in core hive code itself.
    }

    for (int i = 0; i < indexesOnTable.size(); i++) {
      Index index = null;
      index = indexesOnTable.get(i);
      // The handler class implies the type of the index (e.g. compact
      // summary index would be:
      // "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler").
      String indexType = index.getIndexHandlerClass();
      for (int  j = 0; j < matchIndexTypes.size(); j++) {
        if (indexType.equals(matchIndexTypes.get(j))) {
          matchingIndexes.add(index);
          break;
        }
      }
    }
    return matchingIndexes;
  }


  protected boolean shouldApplyOptimization(){
    boolean retValue = true;
    HashMap<String, Operator<? extends Serializable>> topOpMap = parseContext.getTopOps();
    Iterator<String> topOpItr = topOpMap.keySet().iterator();
    while(topOpItr.hasNext()){
      topTable = topOpItr.next();
      Operator<? extends Serializable> topOp = topOpMap.get(topTable);
      checkSingleDAG(topOp);
    }
    retValue = checkIfOptimizationCanApply();
    return retValue;
  }


  private void checkSingleDAG(Operator<? extends Serializable> topOp) {
      NO_OF_TABLES++;
      if(topTable.contains(":")){
        NO_OF_SUBQUERIES++;
        LOG.info("Table - " + topTable + " contains subquery.");
      }
      if(topOp instanceof TableScanOperator){
        TableScanOperator ts = (TableScanOperator) topOp;
        Table tsTable = parseContext.getTopToTable().get(ts);
        if (tsTable != null) {
          List<String> idxType = new ArrayList<String>();
          idxType.add(SUPPORTED_INDEX_TYPE);
          List<Index> indexTables = getIndexes(tsTable, idxType);
          if (indexTables.size() == 0) {
            TABLE_HAS_NO_INDEX = true;
          }else{
            DAGTraversal(topOp.getChildOperators());
          }
        }


      }
  }

  private void DAGTraversal(List<Operator<? extends Serializable>> topOpChildrenList){
    List<Operator<? extends Serializable>> childrenList = topOpChildrenList;
    while(childrenList != null && childrenList.size() > 0){
      for (Operator<? extends Serializable> operator : childrenList) {
        if(null != operator){
          if(operator instanceof JoinOperator){
            QUERY_HAS_JOIN = true;
            return;
          }else if(operator instanceof ExtractOperator){
            QUERY_HAS_SORT_BY = true;
            QUERY_HAS_DISTRIBUTE_BY = true;
            return;
          }
        }
      }
    }
  }

  private boolean checkIfOptimizationCanApply(){
    boolean canApply = false;
    if (NO_OF_TABLES != 1) {
      LOG.info("Query has more than one table " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }
    if (NO_OF_SUBQUERIES != 0) {
      LOG.info("Query has more than one subqueries " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }
    if(QUERY_HAS_JOIN){
      LOG.info("Query has joins, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;

    }
    if (TABLE_HAS_NO_INDEX) {
      LOG.info("Table " + topTable + " does not have compact index. " +
          "Cannot apply " + getName() + " optimization" );
      return canApply;
    }
    if(QUERY_HAS_SORT_BY){
      LOG.info("Query has sortby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }
    if(QUERY_HAS_DISTRIBUTE_BY){
      LOG.info("Query has distributeby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }

    return canApply;

  }


  public class RewriteGBUsingIndexProcCtx implements NodeProcessorCtx {
  }


}

