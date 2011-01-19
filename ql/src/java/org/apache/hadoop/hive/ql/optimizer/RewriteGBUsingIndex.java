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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
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
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  //Context for checking if this optimization can be applied to the input query
  private final RewriteCanApplyCtx canApplyCtx = new RewriteCanApplyCtx();
  //Context for removing the group by construct operators from the operator tree
  private final RewriteRemoveGroupbyCtx removeGbyCtx = new RewriteRemoveGroupbyCtx();
  //Context for appending a subquery to scan over the index table
  private final RewriteIndexSubqueryCtx subqueryCtx = new RewriteIndexSubqueryCtx();

  //Stores the list of top TableScanOperator names for which the rewrite can be applied
  ArrayList<String> tsOpToProcess = new ArrayList<String>();


  private String indexTableName = "";
  private Set<String> indexKeyNames = new LinkedHashSet<String>();

  //Name of the current table on which rewrite is being performed
  private String currentTableName = null;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    try {
      hiveDb = Hive.get(parseContext.getConf());
    } catch (HiveException e) {
      LOG.info("Exception in getting hive conf");
      e.printStackTrace();
    }

     //Set the environment for all contexts
    canApplyCtx.setParseContext(parseContext);
    canApplyCtx.setHiveDb(hiveDb);
    canApplyCtx.setConf(parseContext.getConf());

    removeGbyCtx.setParseContext(parseContext);
    removeGbyCtx.setHiveDb(hiveDb);

    subqueryCtx.setParseContext(parseContext);

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
        indexKeyNames = canApplyCtx.getIndexKeyNames();
        indexTableName = canApplyCtx.getIndexTableName();
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
   * We set the appropriate values in {@link RewriteVars} enum.
   * @return
   */
  private boolean shouldApplyOptimization(){
    boolean canApply = true;

    /*
     * This block of code iterates over the topToTable map from ParseContext
     * to determine if the query has a scan over multiple tables.
     * We do not apply this optimization for this case as of now.
     *
     * */
    HashMap<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
    Iterator<Table> valuesItr = topToTable.values().iterator();
    Set<String> tableNameSet = new HashSet<String>();
    while(valuesItr.hasNext()){
      Table table = valuesItr.next();
      tableNameSet.add(table.getTableName());
    }
    if(tableNameSet.size() > 1){
      canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.QUERY_HAS_MULTIPLE_TABLES, true);
      return canApplyCtx.checkIfOptimizationCanApply();
    }

    /*
     * This code iterates over each TableScanOperator from the topOps map from ParseContext.
     * For each operator tree originating from this top TableScanOperator, we determine
     * if the optimization can be applied. If yes, we add the name of the top table to
     * the tsOpToProcess to apply rewrite later on.
     * */
    HashMap<String,Operator<? extends Serializable>> topOps = parseContext.getTopOps();
    Iterator<TableScanOperator> topOpItr = topToTable.keySet().iterator();
    while(topOpItr.hasNext()){
      canApplyCtx.aggFuncCnt = 0;
      canApplyCtx.setIntVar(parseContext.getConf(), RewriteVars.GBY_KEY_CNT, 0);
      TableScanOperator topOp = topOpItr.next();
      Table table = topToTable.get(topOp);
      currentTableName = table.getTableName();
      canApplyCtx.setCurrentTableName(currentTableName);
      canApply =  checkSingleDAG(topOp);
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
    return canApply;
  }



  /**
   * We retrieve the list of index tables on the current table (represented by the TableScanOperator)
   * and return if there are no index tables to be used for rewriting the input query.
   * Else, we walk the operator tree for which this TableScanOperator is the topOp.
   * At the end, we check if all conditions have passed for rewrite. If yes, we
   * determine if the the index is usable for rewrite. Else, we log the condition which
   * did not meet the rewrite criterion.
   *
   * @param topOp
   * @return
   */
  private boolean checkSingleDAG(TableScanOperator topOp) {
    boolean canApply = true;
    TableScanOperator ts = (TableScanOperator) topOp;
    Table tsTable = parseContext.getTopToTable().get(ts);
    if (tsTable != null) {
      List<String> idxType = new ArrayList<String>();
      idxType.add(canApplyCtx.SUPPORTED_INDEX_TYPE);
      List<Index> indexTables = canApplyCtx.getIndexes(tsTable, idxType);
      if (indexTables.size() == 0) {
        canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.TABLE_HAS_NO_INDEX, true);
        return false;
      }else{
        checkEachDAGOperator(topOp);
      }
      canApply = canApplyCtx.checkIfOptimizationCanApply();
      if(canApply){
        canApply = canApplyCtx.isIndexUsable(indexTables);
      }
    }
    return canApply;
  }


  /**
   * This method walks all the nodes starting from topOp TableScanOperator node
   * and invokes methods from {@link RewriteCanApplyProcFactory} for each of the rules
   * added to the opRules map. We use the {@link DefaultGraphWalker} for a post-order
   * traversal of the operator tree.
   *
   * The methods from {@link RewriteCanApplyProcFactory} set appropriate values in
   * {@link RewriteVars} enum to specify if the criteria for rewrite optimization is met.
   *
   * @param topOp
   */
  private void checkEachDAGOperator(Operator<? extends Serializable> topOp){
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "FIL%"), RewriteCanApplyProcFactory.canApplyOnFilterOperator());
    opRules.put(new RuleRegExp("R2", "GBY%"), RewriteCanApplyProcFactory.canApplyOnGroupByOperator());
    opRules.put(new RuleRegExp("R3", "EXT%"), RewriteCanApplyProcFactory.canApplyOnExtractOperator());
    opRules.put(new RuleRegExp("R4", "SEL%"), RewriteCanApplyProcFactory.canApplyOnSelectOperator());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, canApplyCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }


  /**
   * Default procedure for {@link DefaultRuleDispatcher}
   * @return
   */
  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }



  /**
   * Method to rewrite the input query if all optimization criteria is passed.
   * The method iterates over the tsOpToProcess {@link ArrayList} to apply the rewrites
   *
   * @throws SemanticException
   */
  private void rewriteOriginalQuery() throws SemanticException{
    HashMap<String, Operator<? extends Serializable>> topOpMap = parseContext.getTopOps();
    for (String topOpTableName : tsOpToProcess) {
      currentTableName = topOpTableName;
      canApplyCtx.setCurrentTableName(currentTableName);
      TableScanOperator topOp = (TableScanOperator) topOpMap.get(topOpTableName);

      /* This part of the code checks if the 'REMOVE_GROUP_BY' value in RewriteVars enum is set to true.
       * If yes, it sets the environment for the RewriteRemoveGroupbyCtx context and invokes
       * method to apply rewrite by removing group by construct operators from the original operator tree.
       * */
      if(canApplyCtx.getBoolVar(parseContext.getConf(), RewriteVars.REMOVE_GROUP_BY)){
        removeGbyCtx.setIndexName(indexTableName);
        removeGbyCtx.setOpc(parseContext.getOpParseCtx());
        removeGbyCtx.setCanApplyCtx(canApplyCtx);
        invokeRemoveGbyProc(topOp);
      }

      /* This part of the code checks if the 'SHOULD_APPEND_SUBQUERY' value in RewriteVars enum is set to true.
       * If yes, it sets the environment for the RewriteIndexSubqueryCtx context and invokes
       * method to append a new subquery that scans over the index table rather than the original table.
       * We first create the subquery context, then copy the RowSchema/RowResolver from subquery to original operator tree.
       * */
      if(canApplyCtx.getBoolVar(parseContext.getConf(), RewriteVars.SHOULD_APPEND_SUBQUERY)){
        subqueryCtx.setIndexKeyNames(indexKeyNames);
        subqueryCtx.setIndexName(indexTableName);
        subqueryCtx.setCurrentTableName(currentTableName);
        subqueryCtx.createSubqueryContext();
        HashMap<TableScanOperator, Table> subqTopOpMap = subqueryCtx.getSubqueryPctx().getTopToTable();
        Iterator<TableScanOperator> topOpItr = subqTopOpMap.keySet().iterator();
        TableScanOperator subqTopOp = null;
        if(topOpItr.hasNext()){
          subqTopOp = topOpItr.next();
        }
        invokeSubquerySelectSchemaProc(subqTopOp);
        invokeFixAllOperatorSchemasProc(topOp);
      }

      //Finally we set the enum variables to false
      canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.REMOVE_GROUP_BY, false);
      canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.SHOULD_APPEND_SUBQUERY, false);

    }
    LOG.info("Finished Rewriting query");

  }


  /**
   * Walk the original operator tree using the {@link DefaultGraphWalker} using the rules.
   * Each of the rules invoke respective methods from the {@link RewriteRemoveGroupbyProcFactory}
   * to remove the group-by constructs from the original query and replace the original
   * {@link TableScanOperator} with the new index table scan operator.
   *
   * @param topOp
   * @throws SemanticException
   */
  private void invokeRemoveGbyProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // replace scan operator containing original table with index table
    opRules.put(new RuleRegExp("R1", "TS%"), RewriteRemoveGroupbyProcFactory.getReplaceTableScanProc());
    //rule that replaces index key selection with size(_offsets) function in original query
    opRules.put(new RuleRegExp("R2", "SEL%"), RewriteRemoveGroupbyProcFactory.getReplaceIdxKeyWithSizeFuncProc());
    // remove group-by pattern from original operator tree
    opRules.put(new RuleRegExp("R3", "GBY%RS%GBY%"), RewriteRemoveGroupbyProcFactory.getRemoveGroupByProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, removeGbyCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);

    //Getting back new parseContext and new OpParseContext after GBY-RS-GBY is removed
    parseContext = removeGbyCtx.getParseContext();
    parseContext.setOpParseCtx(removeGbyCtx.getOpc());
    LOG.info("Finished Group by Remove");

  }


  /**
   * Walk the original operator tree using the {@link DefaultGraphWalker} using the rules.
   * Each of the rules invoke respective methods from the {@link RewriteIndexSubqueryProcFactory}
   * to
   * @param topOp
   * @throws SemanticException
   */
  private void invokeSubquerySelectSchemaProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    //removes the subquery FileSinkOperator from subquery OpParseContext as
    //we do not need to append FS operator to original operator tree
    opRules.put(new RuleRegExp("R1", "FS%"), RewriteIndexSubqueryProcFactory.getSubqueryFileSinkProc());
    //copies the RowSchema, outputColumnNames, colList, RowResolver, columnExprMap to RewriteIndexSubqueryCtx data structures
    opRules.put(new RuleRegExp("R2", "SEL%"), RewriteIndexSubqueryProcFactory.getSubquerySelectSchemaProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, subqueryCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);
    LOG.info("Finished Fetchin subquery select schema");

  }



  /**
   * This method appends the subquery operator tree to original operator tree
   * It replaces the original table scan operator with index table scan operator
   * Method also copies the information from {@link RewriteIndexSubqueryCtx} to
   * appropriate operators from the original operator tree
   * @param topOp
   * @throws SemanticException
   */
  private void invokeFixAllOperatorSchemasProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    //appends subquery operator tree to original operator tree
    opRules.put(new RuleRegExp("R1", "TS%"), RewriteIndexSubqueryProcFactory.getAppendSubqueryToOriginalQueryProc());

    //copies RowSchema, outputColumnNames, colList, RowResolver, columnExprMap from RewriteIndexSubqueryCtx data structures
    // to SelectOperator of original operator tree
    opRules.put(new RuleRegExp("R2", "SEL%"), RewriteIndexSubqueryProcFactory.getNewQuerySelectSchemaProc());
    //Manipulates the ExprNodeDesc from FilterOperator predicate list as per colList data structure from RewriteIndexSubqueryCtx
    opRules.put(new RuleRegExp("R3", "FIL%"), RewriteIndexSubqueryProcFactory.getNewQueryFilterSchemaProc());
    //Manipulates the ExprNodeDesc from GroupByOperator aggregation list, parameters list \
    //as per colList data structure from RewriteIndexSubqueryCtx
    opRules.put(new RuleRegExp("R4", "GBY%"), RewriteIndexSubqueryProcFactory.getNewQueryGroupbySchemaProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, subqueryCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    ogw.startWalking(topNodes, null);
    this.parseContext = subqueryCtx.getParseContext();
    LOG.info("Fixed all operator schema");

  }

}

