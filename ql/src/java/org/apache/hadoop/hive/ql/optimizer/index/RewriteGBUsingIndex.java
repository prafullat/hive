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

package org.apache.hadoop.hive.ql.optimizer.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;


/**
 * RewriteGBUsingIndex is implemented as one of the Rule-based Optimizations.
 * Implements optimizations for GroupBy clause rewrite using aggregate index.
 * This optimization rewrites GroupBy query over base table to the query over simple table-scan over
 * index table, if there is index on the group by key(s) or the distinct column(s).
 * E.g.
 * <code>
 *   select key
 *   from table
 *   group by key;
 * </code>
 *  to
 *  <code>
 *   select key
 *   from idx_table;
 *  </code>
 *
 *  The rewrite supports following queries:
 *  <ul>
 *  <li> Queries having only those col refs that are in the index key.
 *  <li> Queries that have index key col refs
 *  <ul>
 *    <li> in SELECT
 *    <li> in WHERE
 *    <li> in GROUP BY
 *  </ul>
 *  <li> Queries with agg func COUNT(literal) or COUNT(index key col ref) in SELECT
 *  <li> Queries with SELECT DISTINCT index_key_col_refs
 *  <li> Queries having a subquery satisfying above condition (only the subquery is rewritten)
 *  </ul>
 *  FUTURE:
 *  <ul>
 *  <li> Many of the checks for above criteria rely on equivalence of expressions,
 *       but such framework/mechanism of expression equivalence
 *       isn't present currently or developed yet.
 *       This needs to be supported in order for better robust checks.
 *       This is critically important for correctness of a query rewrite system.
 *  </ul>
 *
 *  @see RewriteCanApplyCtx
 *  @see RewriteCanApplyProcFactory
 *  @see RewriteParseContextGenerator
 *  @see RewriteQueryUsingAggregateIndexCtx
 *  @see RewriteQueryUsingAggregateIndex
 */

public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private HiveConf hiveConf;
  private static final Log LOG = LogFactory.getLog(RewriteGBUsingIndex.class.getName());

  /*
   * Stores the list of top TableScanOperator names for which the rewrite
   * can be applied and the action that needs to be performed for operator tree
   * starting from this TableScanOperator
   */
  private final Map<String, RewriteCanApplyCtx> tsOpToProcess =
    new LinkedHashMap<String, RewriteCanApplyCtx>();

  //Name of the current table on which rewrite is being performed
  private String baseTableName = null;
  private String indexTableName = null; //index which is used for rewrite

  //Index Validation Variables
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.AggregateIndexHandler";
  private static final String IDX_BUCKET_COL = "_bucketname";
  private static final String IDX_OFFSETS_ARRAY_COL = "_offsets";


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    hiveConf = parseContext.getConf();
    try {
      hiveDb = Hive.get(hiveConf);
    } catch (HiveException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }


    /* Check if the input query is internal query that
     * inserts in table (eg. ALTER INDEX...REBUILD etc.)
     * We do not apply optimization here.
     */
    if(isQueryInsertToTable()){
      return parseContext;
    }else{
      /* Check if the input query passes all the tests to be eligible for a rewrite
       * If yes, rewrite original query; else, return the current parseContext
       * */
      if(shouldApplyOptimization()){
        LOG.info("Rewriting Original Query.");
        rewriteOriginalQuery();
      }
      return parseContext;
    }
  }

  /**
   * Use Query block's parse {@link QBParseInfo} information to check if the input query
   * is an internal SQL.
   * If it is true, we do not apply this optimization.
   * @return
   */
  private boolean isQueryInsertToTable(){
    QBParseInfo qbParseInfo =  parseContext.getQB().getParseInfo();
    return qbParseInfo.isInsertToTable();
  }

  /**
   * We traverse the current operator tree to check for conditions in which the
   * optimization cannot be applied.
   *
   * At the end, we check if all conditions have passed for rewrite. If yes, we
   * determine if the the index is usable for rewrite. Else, we log the condition which
   * did not meet the rewrite criterion.
   *
   * @return
   * @throws SemanticException
   */
  boolean shouldApplyOptimization() throws SemanticException{
    boolean canApply = false;
    if(ifQueryHasMultipleTables()){
      //We do not apply this optimization for this case as of now.
      return false;
    }else{
    /*
     * This code iterates over each TableScanOperator from the topOps map from ParseContext.
     * For each operator tree originating from this top TableScanOperator, we determine
     * if the optimization can be applied. If yes, we add the name of the top table to
     * the tsOpToProcess to apply rewrite later on.
     * */
      HashMap<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
      HashMap<String,Operator<? extends Serializable>> topOps = parseContext.getTopOps();
      Iterator<TableScanOperator> topOpItr = topToTable.keySet().iterator();
      while(topOpItr.hasNext()){
        //Context for checking if this optimization can be applied to the input query
        RewriteCanApplyCtx canApplyCtx = RewriteCanApplyCtx.getInstance(parseContext);

        TableScanOperator topOp = topOpItr.next();
        Table table = topToTable.get(topOp);
        baseTableName = table.getTableName();
        HashMap<Index, Set<String>> indexTableMap = getIndexTableInfoForRewrite(topOp);

        if(indexTableMap != null){
          if(indexTableMap.size() == 0){
            LOG.debug("No Valid Index Found to apply Rewrite, " +
                "skipping " + getName() + " optimization");
            return false;
          } else if(indexTableMap.size() > 1){
            // a cost-based analysis can be done here to choose a
            // single index table amongst all valid indexes to apply rewrite
            // We are not handling this
            LOG.debug("Table has multiple valid index tables to apply rewrite, " +
                "skipping " + getName() + " optimization");
            return false;
          }else{
            canApplyCtx.setBaseTableName(baseTableName);
            canApplyCtx.populateRewriteVars(topOp);

            Iterator<Index> indexMapItr = indexTableMap.keySet().iterator();
            Index index = null;
            while(indexMapItr.hasNext()){
              //we rewrite the original query using the first valid index encountered
              //this can be changed if we have a better mechanism to
              //decide which index will produce a better rewrite
              index = indexMapItr.next();
              canApply = canApplyCtx.isIndexUsableForQueryBranchRewrite(index,
                  indexTableMap.get(index));
              if(canApply){
                canApply = checkIfAllRewriteCriteriaIsMet(canApplyCtx);
                //break here if any valid index is found to apply rewrite
                if(canApply){
                  //check if aggregation function is set.
                  //If not, set it using the only indexed column
                  if(canApplyCtx.getAggFunction() == null){
                    //strip of the start and end braces [...]
                    String aggregationFunction = indexTableMap.get(index).toString();
                    aggregationFunction = aggregationFunction.substring(1,
                        aggregationFunction.length() - 1);
                    canApplyCtx.setAggFunction("_count_Of_" + aggregationFunction + "");
                  }
                }
                break;
              }
            }
            indexTableName = index.getIndexTableName();

            if(canApply && topOps.containsValue(topOp)) {
              Iterator<String> topOpNamesItr = topOps.keySet().iterator();
              while(topOpNamesItr.hasNext()){
                String topOpName = topOpNamesItr.next();
                if(topOps.get(topOpName).equals(topOp)){
                  tsOpToProcess.put(topOpName, canApplyCtx);
                }
              }
            }
          }
       }
     }
    }

    if(tsOpToProcess.size() == 0){
      canApply = false;
    }else{
      canApply = true;
    }
    return canApply;
  }


  /**
   * Method to rewrite the input query if all optimization criteria is passed.
   * The method iterates over the tsOpToProcess {@link ArrayList} to apply the rewrites
   * @throws SemanticException
   *
   */
  @SuppressWarnings("unchecked")
  private void rewriteOriginalQuery() throws SemanticException {
    HashMap<String, Operator<? extends Serializable>> topOpMap =
      (HashMap<String, Operator<? extends Serializable>>) parseContext.getTopOps().clone();
    Iterator<String> tsOpItr = tsOpToProcess.keySet().iterator();

    while(tsOpItr.hasNext()){
      baseTableName = tsOpItr.next();
      RewriteCanApplyCtx canApplyCtx = tsOpToProcess.get(baseTableName);
      TableScanOperator topOp = (TableScanOperator) topOpMap.get(baseTableName);
      RewriteQueryUsingAggregateIndexCtx rewriteQueryCtx =
        RewriteQueryUsingAggregateIndexCtx.getInstance(parseContext, hiveDb,
            indexTableName, baseTableName, canApplyCtx.getAggFunction());
      rewriteQueryCtx.invokeRewriteQueryProc(topOp);
      parseContext = rewriteQueryCtx.getParseContext();
      parseContext.setOpParseCtx(rewriteQueryCtx.getOpc());
    }
    LOG.info("Finished Rewriting query");
  }

  private String getName() {
    return "RewriteGBUsingIndex";
  }


  /**
   * This method logs the reason for which we cannot apply the rewrite optimization.
   * @return
   */
  boolean checkIfAllRewriteCriteriaIsMet(RewriteCanApplyCtx canApplyCtx){
    if (canApplyCtx.isQueryHasDistributeBy()){
      LOG.debug("Query has distributeby clause, " +
          "that is not supported with " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isQueryHasSortBy()){
      LOG.debug("Query has sortby clause, " +
          "that is not supported with " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isQueryHasOrderBy()){
      LOG.debug("Query has orderby clause, " +
          "that is not supported with " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.getAggFuncCnt() > 1){
      LOG.debug("More than 1 agg funcs: " +
          "Not supported by " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isAggFuncIsNotCount()){
      LOG.debug("Agg func other than count is " +
          "not supported by " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isCountOnAllCols()){
      LOG.debug("Currently count function needs group by on key columns. This is a count(*) case., "
          + "Cannot apply this " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isCountOfOne()){
      LOG.debug("Currently count function needs group by on key columns. This is a count(1) case., "
          + "Cannot apply this " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isAggFuncColsFetchException()){
      LOG.debug("Got exception while locating child col refs " +
          "of agg func, skipping " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isWhrClauseColsFetchException()){
      LOG.debug("Got exception while locating child col refs for where clause, "
          + "skipping " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isSelClauseColsFetchException()){
      LOG.debug("Got exception while locating child col refs for select list, "
          + "skipping " + getName() + " optimization");
      return false;
    }
    if (canApplyCtx.isGbyKeysFetchException()){
      LOG.debug("Got exception while locating child col refs for GroupBy key, "
          + "skipping " + getName() + " optimization");
      return false;
    }
    return true;
  }



  /**
   * This block of code iterates over the topToTable map from ParseContext
   * to determine if the query has a scan over multiple tables.
   * @return
   */
  boolean ifQueryHasMultipleTables(){
    HashMap<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
    Iterator<Table> valuesItr = topToTable.values().iterator();
    Set<String> tableNameSet = new HashSet<String>();
    while(valuesItr.hasNext()){
      Table table = valuesItr.next();
      tableNameSet.add(table.getTableName());
    }
    if(tableNameSet.size() > 1){
      LOG.debug("Query has more than one table " +
          "that is not supported with " + getName() + " optimization");
      return true;
    }
    return false;
  }


  /**
  * Given a base table meta data, and a list of index types for which we need to
  * find a matching index, this method returns a list of matching index tables.
  * @param baseTableMetaData
  * @param matchIndexTypes
  * @return
   * @throws SemanticException
  */
 List<Index> getIndexes(Table baseTableMetaData,
     List<String> matchIndexTypes) throws SemanticException {
   List<Index> matchingIndexes = new ArrayList<Index>();
   List<Index> indexesOnTable = null;

   try {
     //this limit parameter is required by metastore API's and acts as a check
     // to avoid huge payloads coming back from thrift
     short maxNumOfIndexes = 1024;
     indexesOnTable = baseTableMetaData.getAllIndexes(maxNumOfIndexes);

   } catch (HiveException e) {
     LOG.error("Could not retrieve indexes on the base table. Check logs for error.");
     LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
     throw new SemanticException(e.getMessage(), e);
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


 /**
  * We retrieve the list of index tables on the current table (represented by the TableScanOperator)
  * which can be used to apply rewrite on the original query
  * and return if there are no index tables to be used for rewriting the input query.
  *
  * @param topOp
  * @return
 * @throws SemanticException
  */
 HashMap<Index, Set<String>> getIndexTableInfoForRewrite(TableScanOperator topOp)
 throws SemanticException {
   HashMap<Index, Set<String>> indexTableMap = null;
   TableScanOperator ts = (TableScanOperator) topOp;
   Table tsTable = parseContext.getTopToTable().get(ts);
   if (tsTable != null) {
     List<String> idxType = new ArrayList<String>();
     idxType.add(SUPPORTED_INDEX_TYPE);
     List<Index> indexTables = getIndexes(tsTable, idxType);
     if (indexTables == null || indexTables.size() == 0) {
       LOG.debug("Table " + baseTableName + " does not have aggregate index. " +
           "Cannot apply " + getName() + " optimization");
     }else{
       indexTableMap = populateIndexToKeysMap(indexTables);
     }
   }
   return indexTableMap;
 }


 /**
  * This code block iterates over indexes on the table and picks
  * up the first index that satisfies the rewrite criteria.
  * @param indexTables
  * @return
  * @throws SemanticException
  */
 HashMap<Index, Set<String>> populateIndexToKeysMap(List<Index> indexTables)
 throws SemanticException{
   Index index = null;
   Hive hiveInstance = hiveDb;
   HashMap<Index, Set<String>> indexToKeysMap = new LinkedHashMap<Index, Set<String>>();

   for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
     final Set<String> indexKeyNames = new LinkedHashSet<String>();
     index = indexTables.get(idxCtr);

     //Getting index key columns
     StorageDescriptor sd = index.getSd();
     List<FieldSchema> idxColList = sd.getCols();
     for (FieldSchema fieldSchema : idxColList) {
       indexKeyNames.add(fieldSchema.getName());
     }

     assert indexKeyNames.size()==1;

     // Check that the index schema is as expected. This code block should
     // catch problems of this rewrite breaking when the AggregateIndexHandler
     // index is changed.
     // This dependency could be better handled by doing init-time check for
     // compatibility instead of this overhead for every rewrite invocation.
     ArrayList<String> idxTblColNames = new ArrayList<String>();
     try {
       Table idxTbl = hiveInstance.getTable(index.getDbName(),
           index.getIndexTableName());
       for (FieldSchema idxTblCol : idxTbl.getCols()) {
         idxTblColNames.add(idxTblCol.getName());
       }
     } catch (HiveException e) {
       LOG.error("Got exception while locating index table, " +
           "skipping " + getName() + " optimization");
       LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
       throw new SemanticException(e.getMessage(), e);
     }
     assert(idxTblColNames.contains(IDX_BUCKET_COL));
     assert(idxTblColNames.contains(IDX_OFFSETS_ARRAY_COL));

     // we add all index tables which can be used for rewrite
     // and defer the decision of using a particular index for later
     // this is to allow choosing a index if a better mechanism is
     // designed later to chose a better rewrite
     indexToKeysMap.put(index, indexKeyNames);
   }
   return indexToKeysMap;
 }

}

