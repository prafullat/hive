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
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount.GenericUDAFCountEvaluator;


public class RewriteGBUsingIndex implements Transform {
  private static int count = 0;
  private ParseContext parseContext;
  private Hive hiveDb;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;
  private final LinkedHashMap<String, String> colInternalToExternalMap = new LinkedHashMap<String, String>();
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  //vars
  int NO_OF_SUBQUERIES = 0;
  int NO_OF_TABLES = 0;
  boolean QUERY_HAS_JOIN = false;
  boolean TABLE_HAS_NO_INDEX = false;
  boolean QUERY_HAS_SORT_BY = false;
  boolean QUERY_HAS_ORDER_BY = false;
  boolean QUERY_HAS_DISTRIBUTE_BY = false;
  boolean QUERY_HAS_GROUP_BY = false;
  boolean QUERY_HAS_DISTINCT = false;
  int AGG_FUNC_CNT = 0;
  int GBY_KEY_CNT = 0;
  boolean AGG_FUNC_IS_NOT_COUNT = false;
  boolean AGG_FUNC_COLS_FETCH_EXCEPTION = false;
  boolean WHR_CLAUSE_COLS_FETCH_EXCEPTION = false;
  boolean SEL_CLAUSE_COLS_FETCH_EXCEPTION = false;
  boolean GBY_KEYS_FETCH_EXCEPTION = false;
  boolean GBY_KEY_HAS_NON_INDEX_COLS = false;
  boolean SEL_HAS_NON_COL_REF = false;
  boolean GBY_NOT_ON_COUNT_KEYS = false;
  boolean IDX_TBL_SEARCH_EXCEPTION = false;
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
  private static final String COMPACT_IDX_BUCKET_COL = "_bucketname";
  private static final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";
  private String topTable = null;
  private final List<String> selColRefNameList = new ArrayList<String>();
  private List<String> predColRefs = new ArrayList<String>();
  private final List<String> gbKeyNameList = new ArrayList<String>();
  private final RewriteGBUsingIndexProcCtx context = new RewriteGBUsingIndexProcCtx();
  private final List<List<AggregationDesc>> colRefAggFuncInputList = new ArrayList<List<AggregationDesc>>();

  RewriteGBUsingIndex(){
    count++;
  }

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

    if(count == 3){
      if( shouldApplyOptimization() == true ) {
        return parseContext;
      }else{
        return null;
      }

    }

    return parseContext;
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
      retValue = checkSingleDAG(topOp);
      if(!retValue) {
        break;
      }
    }
    retValue = checkIfOptimizationCanApply();
    return retValue;
  }


  private boolean checkSingleDAG(Operator<? extends Serializable> topOp) {
    boolean result = true;
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
            return false;
          }else{
            result = DAGTraversal(topOp.getChildOperators(), indexTables);
          }
        }
      }
      return result;
  }

  private boolean DAGTraversal(List<Operator<? extends Serializable>> topOpChildrenList, List<Index> indexTables){
    boolean result = true;
    List<Operator<? extends Serializable>> childrenList = topOpChildrenList;
    while(childrenList != null && childrenList.size() > 0){
      for (Operator<? extends Serializable> operator : childrenList) {
        if(null != operator){
          if(operator instanceof JoinOperator){
            QUERY_HAS_JOIN = true;
            return false;
          }else if(operator instanceof ExtractOperator){
            result = processExtractOperator(operator);
          }else if(operator instanceof GroupByOperator){
            result = processGroupByOperator(operator);
          }else if(operator instanceof FilterOperator){
            result = processFilterOperator(operator);
          }else if(operator instanceof SelectOperator){
            result = processSelectOperator(operator, indexTables);
         }
          if(!result){
            childrenList = null;
            break;
          }else{
            childrenList = operator.getChildOperators();
          }
        }
      }
    }

    //result = checkIndexInformation(indexTables);
    return result;
  }


  private boolean processGroupByOperator(Operator<? extends Serializable> operator){
    if(parseContext.getGroupOpToInputTables().containsKey(operator)){
      QUERY_HAS_GROUP_BY = true;
      GroupByDesc conf = (GroupByDesc) operator.getConf();
      ArrayList<AggregationDesc> aggrList = conf.getAggregators();
      if(aggrList != null && aggrList.size() > 0){
          for (AggregationDesc aggregationDesc : aggrList) {
            AGG_FUNC_CNT++;
            if(AGG_FUNC_CNT > 1) {
              return false;
            }
            String aggFunc = aggregationDesc.getGenericUDAFName();
            if(!aggFunc.equals("count")){
              AGG_FUNC_IS_NOT_COUNT = true;
              return false;
            }else{
              GenericUDAFEvaluator aggEval = aggregationDesc.getGenericUDAFEvaluator();
              if(aggEval instanceof GenericUDAFCountEvaluator){
                LOG.info("here");
              }
              ArrayList<ExprNodeDesc> para = aggregationDesc.getParameters();
              if(para == null || para.size() == 0){
                AGG_FUNC_COLS_FETCH_EXCEPTION =  true;
                return false;
              }else{
                ExprNodeDesc end = para.get(0);
                if(end instanceof ExprNodeColumnDesc){
                  LOG.info("Code to specify GBY_NOT_ON_CNT_KEYS");
                }else if(end instanceof ExprNodeConstantDesc){
                  GBY_NOT_ON_COUNT_KEYS = true;
                  return false;
                }
              }
            }
            colRefAggFuncInputList.add(aggrList);
          }
      }else{
        QUERY_HAS_DISTINCT = true;
        return false;
      }
      ArrayList<ExprNodeDesc> keyList = conf.getKeys();
      if(keyList == null || keyList.size() == 0){
        GBY_KEYS_FETCH_EXCEPTION = true;
        return false;
      }
      GBY_KEY_CNT = keyList.size();
      for (ExprNodeDesc exprNodeDesc : keyList) {
        gbKeyNameList.addAll(exprNodeDesc.getCols());
      }

    }
    return true;
  }

  private boolean processExtractOperator(Operator<? extends Serializable> operator){
    if(operator.getParentOperators() != null && operator.getParentOperators().size() >0){
      Operator<? extends Serializable> interim = operator.getParentOperators().get(0);
      if(interim instanceof ReduceSinkOperator){
        ReduceSinkDesc conf = (ReduceSinkDesc) interim.getConf();
        ArrayList<ExprNodeDesc> partCols = conf.getPartitionCols();
        int nr = conf.getNumReducers();
        if(nr == -1){
          if(partCols != null && partCols.size() > 0){
            QUERY_HAS_DISTRIBUTE_BY = true;
            return false;
          }else{
            QUERY_HAS_SORT_BY = true;
            return false;
          }
        }else if(nr == 1){
          QUERY_HAS_ORDER_BY = true;
          return false;
        }

      }
    }
    return true;
  }

  private boolean processFilterOperator(Operator<? extends Serializable> operator){
    FilterDesc conf = (FilterDesc)operator.getConf();
    ExprNodeGenericFuncDesc oldengfd = (ExprNodeGenericFuncDesc) conf.getPredicate();
    if(oldengfd == null){
      WHR_CLAUSE_COLS_FETCH_EXCEPTION = true;
      return false;
    }
    List<String> colList = oldengfd.getCols();
    if(colList == null || colList.size() == 0){
      WHR_CLAUSE_COLS_FETCH_EXCEPTION = true;
      return false;
    }
    predColRefs = colList;
    return true;
  }

  private boolean processSelectOperator(Operator<? extends Serializable> operator, List<Index> indexTables){
    List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
    Operator<? extends Serializable> child = childrenList.get(0);
    if(child instanceof GroupByOperator){
      SelectDesc conf = (SelectDesc)operator.getConf();
      ArrayList<ExprNodeDesc> selColList = conf.getColList();
      int colCnt = 0;
      if(selColList != null && selColList.size() != 0){
        for (ExprNodeDesc exprNodeDesc : selColList) {
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            List<String> exprColList = exprNodeDesc.getCols();
            if(!((ExprNodeColumnDesc) exprNodeDesc).getIsPartitionColOrVirtualCol()){
              for (String colName : exprColList) {
                colInternalToExternalMap.put("_col"+ colCnt++, colName);
              }
            }
          }
        }
      }
      return true;
    }else{
      int colSelCnt = 0;
      int colIdxCnt = 0;
      List<String> indexSourceCols = new ArrayList<String>();
      for (int i = 0; i < indexTables.size(); i++) {
        Index index = null;
        index = indexTables.get(i);
        StorageDescriptor sd = index.getSd();
        List<FieldSchema> idxColList = sd.getCols();
        for (FieldSchema fieldSchema : idxColList) {
          indexSourceCols.add(fieldSchema.getName());
        }

      }
      SelectDesc conf = (SelectDesc)operator.getConf();
      ArrayList<ExprNodeDesc> selColList = conf.getColList();

      if(selColList == null || selColList.size() == 0){
        SEL_CLAUSE_COLS_FETCH_EXCEPTION = true;
        return false;
      }
      for (ExprNodeDesc exprNodeDesc : selColList) {
        List<String> exprColList = exprNodeDesc.getCols();
        for (String colName : exprColList) {
          colSelCnt++;
          if(QUERY_HAS_GROUP_BY && colInternalToExternalMap != null && colInternalToExternalMap.size() > 0){
            String extName = colInternalToExternalMap.get(colName);
            if(indexSourceCols.contains(extName)){
              colIdxCnt++;
            }
          }else{
            if(indexSourceCols.contains(colName)){
              colIdxCnt++;
            }
          }
        }
        selColRefNameList.addAll(exprColList);
      }
      if(colIdxCnt != colSelCnt){
        SEL_HAS_NON_COL_REF = true;
        return false;
      }
    }

    return true;
  }

  private boolean checkIndexInformation(List<Index> indexTables){
    Index idx = null;
    Hive hiveInstance = hiveDb;

    // This code block iterates over indexes on the table and picks up the
    // first index that satisfies the rewrite criteria.
    for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      boolean removeGroupBy = true;
      boolean optimizeCount = false;

      idx = indexTables.get(idxCtr);

      //Getting index key columns
      List<Order> idxColList = idx.getSd().getSortCols();

      Set<String> idxKeyColsNames = new TreeSet<String>();
      for (int i = 0;i < idxColList.size(); i++) {
        idxKeyColsNames.add(idxColList.get(i).getCol().toLowerCase());
      }

      // Check that the index schema is as expected. This code block should
      // catch problems of this rewrite breaking when the CompactIndexHandler
      // index is changed.
      // This dependency could be better handled by doing init-time check for
      // compatibility instead of this overhead for every rewrite invocation.
      ArrayList<String> idxTblColNames = new ArrayList<String>();
      try {
        Table idxTbl = hiveInstance.getTable(idx.getDbName(),
            idx.getIndexTableName());
        for (FieldSchema idxTblCol : idxTbl.getCols()) {
          idxTblColNames.add(idxTblCol.getName());
        }
      } catch (HiveException e) {
        IDX_TBL_SEARCH_EXCEPTION = true;
        return false;
      }
      assert(idxTblColNames.contains(COMPACT_IDX_BUCKET_COL));
      assert(idxTblColNames.contains(COMPACT_IDX_OFFSETS_ARRAY_COL));
      assert(idxTblColNames.size() == idxKeyColsNames.size() + 2);

      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if (idxKeyColsNames.containsAll(selColRefNameList) == false) {
        LOG.info("Select list has non index key column : " +
            " Cannot use this index  " + idx.getIndexName());
        continue;
      }

      // We need to check if all columns from index appear in select list only
      // in case of DISTINCT queries, In case group by queries, it is okay as long
      // as all columns from index appear in group-by-key list.
      if (QUERY_HAS_DISTINCT) {
        // Check if all columns from index are part of select list too
        if (selColRefNameList.containsAll(idxKeyColsNames) == false)  {
          LOG.info("Index has non select list columns " +
              " Cannot use this index  " + idx.getIndexName());
          continue;
        }
      }

      //--------------------------------------------
      // Check if all columns in where predicate are part of index key columns
      // TODO: Currently we allow all predicates , would it be more efficient
      // (or at least not worse) to read from index_table and not from baseTable?
      if (idxKeyColsNames.containsAll(predColRefs) == false) {
        LOG.info("Predicate column ref list has non index key column : " +
            " Cannot use this index  " + idx.getIndexName());
        continue;
      }

      if (!QUERY_HAS_DISTINCT)  {
        //--------------------------------------------
        // For group by, we need to check if all keys are from index columns
        // itself. Here GB key order can be different than index columns but that does
        // not really matter for final result.
        // E.g. select c1, c2 from src group by c2, c1;
        // we can rewrite this one to:
        // select c1, c2 from src_cmpt_idx;
        if (idxKeyColsNames.containsAll(gbKeyNameList) == false) {
          GBY_KEY_HAS_NON_INDEX_COLS = true;
          return false;
        }

        if (gbKeyNameList.containsAll(idxKeyColsNames) == false)  {
          // GB key and idx key are not same, don't remove GroupBy, but still do index scan
          removeGroupBy = false;
        }

        // This check prevents to remove GroupBy for cases where the GROUP BY key cols are
        // not simple expressions i.e. simple index key cols (in any order), but some
        // expressions on the the key cols.
        // e.g.
        // 1. GROUP BY key, f(key)
        //     FUTURE: If f(key) output is functionally dependent on key, then we should support
        //            it. However we don't have mechanism/info about f() yet to decide that.
        // 2. GROUP BY idxKey, 1
        //     FUTURE: GB Key has literals along with idxKeyCols. Develop a rewrite to eliminate the
        //            literals from GB key.
        // 3. GROUP BY idxKey, idxKey
        //     FUTURE: GB Key has dup idxKeyCols. Develop a rewrite to eliminate the dup key cols
        //            from GB key.
        if (QUERY_HAS_GROUP_BY &&
            gbKeyNameList.size() != GBY_KEY_CNT) {
          LOG.info("Group by key has only some non-indexed columns, GroupBy will be"
              + " preserved by rewrite " + getName() + " optimization" );
          removeGroupBy = false;
        }

        // FUTURE: See if this can be relaxed.
        // If we have agg function (currently only COUNT is supported), check if its input are
        // from index. we currently support only that.
        if (colRefAggFuncInputList.size() > 0)  {
          for (int aggFuncIdx = 0; aggFuncIdx < colRefAggFuncInputList.size(); aggFuncIdx++)  {
            if (idxKeyColsNames.containsAll(colRefAggFuncInputList.get(aggFuncIdx)) == false) {
              LOG.info("Agg Func input is not present in index key columns. Currently " +
                  "only agg func on index columns are supported by rewrite " + getName() + " optimization" );
              continue;
            }

            // If we have count on some key, check if key is same as index key,
            if (colRefAggFuncInputList.get(aggFuncIdx).size() > 0)  {
              if (colRefAggFuncInputList.get(aggFuncIdx).containsAll(idxKeyColsNames))  {
                optimizeCount = true;
              }
            }
            else  {
              optimizeCount = true;
            }
          }
        }
      }

      //Now that we are good to do this optimization, set parameters in context
      //which would be used by transforation proceduer as inputs.

      //subquery is needed only in case of optimizecount and complex gb keys?
      if( !(optimizeCount == true && removeGroupBy == false) ) {
        context.addTable(topTable, idx.getIndexTableName());
      }

    }
    return true;

  }




  private boolean checkIfOptimizationCanApply(){
    boolean canApply = false;
    if (NO_OF_TABLES != 1) {
      LOG.info("Query has more than one table " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }//1
    if (NO_OF_SUBQUERIES != 0) {
      LOG.info("Query has more than one subqueries " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }//2
    if(QUERY_HAS_JOIN){
      LOG.info("Query has joins, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;

    }//3
    if (TABLE_HAS_NO_INDEX) {
      LOG.info("Table " + topTable + " does not have compact index. " +
          "Cannot apply " + getName() + " optimization" );
      return canApply;
    }//4
    if(QUERY_HAS_DISTRIBUTE_BY){
      LOG.info("Query has distributeby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }//5
    if(QUERY_HAS_SORT_BY){
      LOG.info("Query has sortby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }//6
    if(QUERY_HAS_ORDER_BY){
      LOG.info("Query has orderby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return canApply;
    }//7
    if(AGG_FUNC_CNT > 1){
      LOG.info("More than 1 agg funcs: " +
      		"Not supported by " + getName() + " optimization" );
      return canApply;
    }//8
    if(AGG_FUNC_IS_NOT_COUNT){
      LOG.info("Agg func other than count is " +
      		"not supported by " + getName() + " optimization" );
      return canApply;
    }//9
    if(AGG_FUNC_COLS_FETCH_EXCEPTION){
      LOG.info("Got exception while locating child col refs " +
      		"of agg func, skipping " + getName() + " optimization" );
      return canApply;
    }//10
    if(WHR_CLAUSE_COLS_FETCH_EXCEPTION){
      LOG.info("Got exception while locating child col refs for where clause, "
          + "skipping " + getName() + " optimization" );
      return canApply;
    }//11
    if(QUERY_HAS_DISTINCT){
      LOG.info("Select-list has distinct. " +
          "Cannot apply the rewrite " + getName() + " optimization" );
      return canApply;
    }//12
    if(SEL_HAS_NON_COL_REF){
      LOG.info("Select-list has some non-col-ref expression. " +
          "Cannot apply the rewrite " + getName() + " optimization" );
      return canApply;
    }//13
    if(SEL_CLAUSE_COLS_FETCH_EXCEPTION){
      LOG.info("Got exception while locating child col refs for select list, "
          + "skipping " + getName() + " optimization" );
      return canApply;
    }//14
    if(GBY_KEYS_FETCH_EXCEPTION){
      LOG.info("Got exception while locating child col refs for GroupBy key, "
          + "skipping " + getName() + " optimization" );
      return canApply;
    }//15
    if(GBY_NOT_ON_COUNT_KEYS){
      LOG.info("Currently count function needs group by on key columns, "
          + "Cannot apply this " + getName() + " optimization" );
      return canApply;
    }//16
    if(IDX_TBL_SEARCH_EXCEPTION){
      LOG.info("Got exception while locating index table, " +
      		"skipping " + getName() + " optimization" );
      return canApply;
    }//17
    if(GBY_KEY_HAS_NON_INDEX_COLS){
      LOG.info("Group by key has some non-indexed columns, " +
      		"Cannot apply rewrite " + getName() + " optimization" );
      return canApply;
    }//18
    canApply = true;
    return canApply;

  }


  public class RewriteGBUsingIndexProcCtx implements NodeProcessorCtx {
    private String indexTableName;
    private ParseContext parseContext;
    private Hive hiveDb;
    private boolean replaceTableWithIdxTable;


    /**
     * True if the base table has compact summary index associated with it
     */
    private boolean hasValidTableScan;

    //Map for base table to index table mapping
    //TableScan operator for base table will be modified to read from index table
    private final HashMap<String, String> baseToIdxTableMap;

    public RewriteGBUsingIndexProcCtx() {
      baseToIdxTableMap = new HashMap<String, String>();
    }

    public void addTable(String baseTableName, String indexTableName) {
      baseToIdxTableMap.put(baseTableName, indexTableName);
    }

    public String findBaseTable(String baseTableName)  {
      return baseToIdxTableMap.get(baseTableName);
    }

  }


}

