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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.RewriteCanApplyCtx.RewriteVars;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;


/**
 * RewriteGBUsingIndex is implemented as one of the Rule-based Optimizations.
 * Implements optimizations for GroupBy clause rewrite using compact index.
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
 *  The rewrite supports following queries
 *  - Queries having only those col refs that are in the index key.
 *  - Queries that have index key col refs
 *    - in SELECT
 *    - in WHERE
 *    - in GROUP BY
 *  - Queries with agg func COUNT(literal) or COUNT(index key col ref)
 *    in SELECT
 *  - Queries with SELECT DISTINCT index_key_col_refs
 *  - Queries having a subquery satisfying above condition (only the
 *    subquery is rewritten)
 *
 *  FUTURE:
 *  - Many of the checks for above criteria rely on equivalence of expressions,
 *    but such framework/mechanism of expression equivalence isn't present currently or developed yet.
 *    This needs to be supported in order for better robust checks. This is critically important for
 *    correctness of a query rewrite system.
 *  - This code currently uses index types with org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler.
 *    However, the  CompactIndexHandler currently stores the distinct block offsets and not the row offsets.
 *    Use of this index type will give erroneous results to compute COUNT if the same key appears more
 *    than once within the same block. To address this issue, we plan to create a new index type in future.
 *
 *
 *  @see RewriteCanApplyCtx
 *  @see RewriteCanApplyProcFactory
 *  @see RewriteRemoveGroupbyCtx
 *  @see RewriteRemoveGroupbyProcFactory
 *  @see RewriteIndexSubqueryCtx
 *  @see RewriteIndexSubqueryProcFactory
 *  @see RewriteParseContextGenerator
 *
 */
public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private HiveConf hiveConf;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  //Context for checking if this optimization can be applied to the input query
  private RewriteCanApplyCtx canApplyCtx;
  //Context for removing the group by construct operators from the operator tree
  private RewriteRemoveGroupbyCtx removeGbyCtx;
  //Context for appending a subquery to scan over the index table
  private RewriteIndexSubqueryCtx subqueryCtx;

  //Stores the list of top TableScanOperator names for which the rewrite can be applied and the action that needs to be performed for operator tree
  //starting from this TableScanOperator
  private final List<String> tsOpToProcess = new ArrayList<String>();

  private HashMap<String, Set<String>> indexTableMap = new LinkedHashMap<String, Set<String>>();

  //Name of the current table on which rewrite is being performed
  private String currentTableName = null;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    hiveConf = parseContext.getConf();
    try {
      hiveDb = Hive.get(hiveConf);
    } catch (HiveException e) {
      LOG.info("Exception in getting hive conf");
      e.printStackTrace();
    }


    /* Check if the input query is internal query that inserts in table (eg. ALTER INDEX...REBUILD etc.)
     * We do not apply optimization here.
     * */
    if(isQueryInsertToTable()){
        return parseContext;
    }else{
      /* Check if the input query passes all the tests to be eligible for a rewrite
       * If yes, rewrite original query; else, return the current parseContext
       * */
      if(shouldApplyOptimization()){
        LOG.debug("Rewriting Original Query.");
        if(indexTableMap.size() > 1){
          LOG.info("Table has multiple valid index tables to apply rewrite.");
        }

        Iterator<String> indexMapItr = indexTableMap.keySet().iterator();
        while(indexMapItr.hasNext()){
          rewriteOriginalQuery(indexMapItr.next());
          //we rewrite the original query using the first valid index encountered
          //this can be changed if we have a better mechanism to decide which index will produce a better rewrite
          break;
        }
        canApplyCtx.resetRewriteVars();
        canApplyCtx.resetCanApplyCtx();

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
   */
  boolean shouldApplyOptimization(){
    //Set the environment for all contexts
    canApplyCtx = RewriteCanApplyCtx.getInstance(parseContext, hiveDb, hiveConf);

    boolean canApply = true;
    if(canApplyCtx.ifQueryHasMultipleTables()){
      //We do not apply this optimization for this case as of now.
      canApply = checkIfAllRewriteCriteriaIsMet();
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
        canApplyCtx.resetRewriteVars();
        canApplyCtx.resetCanApplyCtx();

        TableScanOperator topOp = topOpItr.next();
        Table table = topToTable.get(topOp);
        currentTableName = table.getTableName();
        canApplyCtx.setCurrentTableName(currentTableName);
        canApplyCtx.populateRewriteVars(topOp);
        indexTableMap = canApplyCtx.getIndexTableInfoForRewrite(topOp);

        canApply = checkIfAllRewriteCriteriaIsMet();
        if(canApply && topOps.containsValue(topOp)) {
          Iterator<String> topOpNamesItr = topOps.keySet().iterator();
          while(topOpNamesItr.hasNext()){
            String topOpName = topOpNamesItr.next();
            if(topOps.get(topOpName).equals(topOp)){
              tsOpToProcess.add(topOpName);
            }
          }
        }
     }
    }
    return canApply;
  }


  /**
   * Method to rewrite the input query if all optimization criteria is passed.
   * The method iterates over the tsOpToProcess {@link ArrayList} to apply the rewrites
   *
   * @throws SemanticException
   */
  private void rewriteOriginalQuery(String indexTableName) throws SemanticException{
    HashMap<String, Operator<? extends Serializable>> topOpMap = parseContext.getTopOps();
    for (String topOpTableName : tsOpToProcess) {
      currentTableName = topOpTableName;
      TableScanOperator topOp = (TableScanOperator) topOpMap.get(topOpTableName);

      /* This part of the code checks if the 'REMOVE_GROUP_BY' value in RewriteVars enum is set to true.
       * If yes, it sets the environment for the RewriteRemoveGroupbyCtx context and invokes
       * method to apply rewrite by removing group by construct operators from the original operator tree.
       * */
      if(canApplyCtx.getBoolVar(hiveConf, RewriteVars.REMOVE_GROUP_BY)){
        removeGbyCtx = RewriteRemoveGroupbyCtx.getInstance(parseContext, hiveDb, indexTableName);
        removeGbyCtx.invokeRemoveGbyProc(topOp);
        //Getting back new parseContext and new OpParseContext after GBY-RS-GBY is removed
        parseContext = removeGbyCtx.getParseContext();
        parseContext.setOpParseCtx(removeGbyCtx.getOpc());
        LOG.info("Finished Group by Remove");
      }

      /* This part of the code checks if the 'SHOULD_APPEND_SUBQUERY' value in RewriteVars enum is set to true.
       * If yes, it sets the environment for the RewriteIndexSubqueryCtx context and invokes
       * method to append a new subquery that scans over the index table rather than the original table.
       * We first create the subquery context, then copy the RowSchema/RowResolver from subquery to original operator tree.
       * */
      if(canApplyCtx.getBoolVar(hiveConf, RewriteVars.SHOULD_APPEND_SUBQUERY)){
        subqueryCtx = RewriteIndexSubqueryCtx.getInstance(parseContext, indexTableName, currentTableName,
            canApplyCtx.getSelectColumnsList());
        subqueryCtx.createSubqueryContext();

        HashMap<TableScanOperator, Table> subqTopOpMap = subqueryCtx.getSubqueryPctx().getTopToTable();
        Iterator<TableScanOperator> topOpItr = subqTopOpMap.keySet().iterator();
        TableScanOperator subqTopOp = null;
        if(topOpItr.hasNext()){
          subqTopOp = topOpItr.next();
        }
        subqueryCtx.invokeSubquerySelectSchemaProc(subqTopOp);
        LOG.info("Finished Fetching subquery select schema");

        subqueryCtx.invokeFixAllOperatorSchemasProc(topOp);
        parseContext = subqueryCtx.getParseContext();
        LOG.info("Finished appending subquery");
      }

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
  boolean checkIfAllRewriteCriteriaIsMet(){
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.QUERY_HAS_MULTIPLE_TABLES)) {
      LOG.info("Query has more than one table " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.TABLE_HAS_NO_INDEX)) {
      LOG.info("Table " + currentTableName + " does not have compact index. " +
          "Cannot apply " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.QUERY_HAS_DISTRIBUTE_BY)){
      LOG.info("Query has distributeby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.QUERY_HAS_SORT_BY)){
      LOG.info("Query has sortby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.QUERY_HAS_ORDER_BY)){
      LOG.info("Query has orderby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getIntVar(hiveConf, RewriteVars.AGG_FUNC_CNT) > 1 ){
      LOG.info("More than 1 agg funcs: " +
          "Not supported by " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.AGG_FUNC_IS_NOT_COUNT)){
      LOG.info("Agg func other than count is " +
          "not supported by " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.COUNT_ON_ALL_COLS)){
      LOG.info("Currently count function needs group by on key columns. This is a count(*) case., "
          + "Cannot apply this " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs " +
          "of agg func, skipping " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for where clause, "
          + "skipping " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.SEL_CLAUSE_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for select list, "
          + "skipping " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.GBY_KEYS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for GroupBy key, "
          + "skipping " + getName() + " optimization" );
      return false;
    }
    if (canApplyCtx.getBoolVar(hiveConf, RewriteVars.IDX_TBL_SEARCH_EXCEPTION)){
      LOG.info("Got exception while locating index table, " +
          "skipping " + getName() + " optimization" );
      return false;
    }
    if(indexTableMap.size() == 0){
      LOG.info("No Valid Index Found to apply Rewrite, " +
          "skipping " + getName() + " optimization" );
      return false;
    }

    return true;
  }





}

