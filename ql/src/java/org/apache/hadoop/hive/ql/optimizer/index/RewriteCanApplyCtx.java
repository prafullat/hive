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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.Operator;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * RewriteCanApplyCtx class stores the context for the {@link RewriteCanApplyProcFactory} to determine
 * if any index can be used and if the input query meets all the criteria for rewrite optimization.
 */
public final class RewriteCanApplyCtx implements NodeProcessorCtx {

  protected final  Log LOG = LogFactory.getLog(RewriteCanApplyCtx.class.getName());

  private RewriteCanApplyCtx(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  public static RewriteCanApplyCtx getInstance(ParseContext parseContext){
    return new RewriteCanApplyCtx(parseContext);
  }

  // Rewrite Variables
  public int aggFuncCnt = 0;
  public int gbyKeyCnt = 0;
  public boolean queryHasSortBy = false;
  public boolean queryHasOrderBy = false;
  public boolean queryHasDistributeBy = false;
  public boolean queryHasGroupBy = false;
  public boolean aggFuncIsNotCount = false;
  public boolean aggFuncColsFetchException = false;
  public boolean whrClauseColsFetchException = false;
  public boolean selClauseColsFetchException = false;
  public boolean gbyKeysFetchException = false;
  public boolean countOnAllCols = false;
  public boolean queryHasGenericUdfOnGroupbyKey = false;
  public boolean queryHasMultipleTables = false;
  public boolean shouldAppendSubquery = false;
  public boolean removeGroupBy = false;


   //Data structures that are populated in the RewriteCanApplyProcFactory methods to check if the index key meets all criteria
   Set<String> selectColumnsList = new LinkedHashSet<String>();
   Set<String> predicateColumnsList = new LinkedHashSet<String>();
   Set<String> gbKeyNameList = new LinkedHashSet<String>();
   Set<String> aggFuncColList = new LinkedHashSet<String>();


   private final ParseContext parseContext;
   private String baseTableName = "";

   void resetCanApplyCtx(){
     aggFuncCnt = 0;
     gbyKeyCnt = 0;
     queryHasSortBy = false;
     queryHasOrderBy = false;
     queryHasDistributeBy = false;
     queryHasGroupBy = false;
     aggFuncIsNotCount = false;
     aggFuncColsFetchException = false;
     whrClauseColsFetchException = false;
     selClauseColsFetchException = false;
     gbyKeysFetchException = false;
     countOnAllCols = false;
     queryHasGenericUdfOnGroupbyKey = false;
     queryHasMultipleTables = false;
     shouldAppendSubquery = false;
     removeGroupBy = false;

     aggFuncCnt = 0;
     selectColumnsList.clear();
     predicateColumnsList.clear();
     gbKeyNameList.clear();
     aggFuncColList.clear();
     baseTableName = "";
   }

  public Set<String> getSelectColumnsList() {
    return selectColumnsList;
  }

  public void setSelectColumnsList(Set<String> selectColumnsList) {
    this.selectColumnsList = selectColumnsList;
  }

  public Set<String> getPredicateColumnsList() {
    return predicateColumnsList;
  }

  public void setPredicateColumnsList(Set<String> predicateColumnsList) {
    this.predicateColumnsList = predicateColumnsList;
  }

  public Set<String> getGbKeyNameList() {
    return gbKeyNameList;
  }

  public void setGbKeyNameList(Set<String> gbKeyNameList) {
    this.gbKeyNameList = gbKeyNameList;
  }

  public Set<String> getAggFuncColList() {
    return aggFuncColList;
  }

  public void setAggFuncColList(Set<String> aggFuncColList) {
    this.aggFuncColList = aggFuncColList;
  }

   public int getAggFuncCnt() {
    return aggFuncCnt;
  }

  public void setAggFuncCnt(int aggFuncCnt) {
    this.aggFuncCnt = aggFuncCnt;
  }

  public String getBaseTableName() {
    return baseTableName;
  }

  public void setBaseTableName(String baseTableName) {
    this.baseTableName = baseTableName;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }


  /**
   * This method walks all the nodes starting from topOp TableScanOperator node
   * and invokes methods from {@link RewriteCanApplyProcFactory} for each of the rules
   * added to the opRules map. We use the {@link DefaultGraphWalker} for a post-order
   * traversal of the operator tree.
   *
   * The methods from {@link RewriteCanApplyProcFactory} set appropriate values in
   * {@link RewriteVars} enum.
   *
   * @param topOp
   */
  void populateRewriteVars(Operator<? extends Serializable> topOp){
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "FIL%"), RewriteCanApplyProcFactory.canApplyOnFilterOperator());
    opRules.put(new RuleRegExp("R2", "GBY%"), RewriteCanApplyProcFactory.canApplyOnGroupByOperator());
    opRules.put(new RuleRegExp("R3", "RS%OP%"), RewriteCanApplyProcFactory.canApplyOnExtractOperator());
    opRules.put(new RuleRegExp("R4", "SEL%"), RewriteCanApplyProcFactory.canApplyOnSelectOperator());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      LOG.warn("Exception in walking operator tree. Rewrite variables not populated", e);

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


  //Map for base table to index table mapping
  //TableScan operator for base table will be modified to read from index table
  private final HashMap<String, String> baseToIdxTableMap = new HashMap<String, String>();;


  public void addTable(String baseTableName, String indexTableName) {
     baseToIdxTableMap.put(baseTableName, indexTableName);
   }

   public String findBaseTable(String baseTableName)  {
     return baseToIdxTableMap.get(baseTableName);
   }


  boolean isIndexUsableForQueryBranchRewrite(Index index, Set<String> indexKeyNames){
    boolean optimizeCount = false;
    removeGroupBy = true;
    //--------------------------------------------
    //Check if all columns in select list are part of index key columns
    if (!indexKeyNames.containsAll(selectColumnsList)) {
      LOG.info("Select list has non index key column : " +
          " Cannot use index " + index.getIndexName());
      return false;
    }

    //--------------------------------------------
    // Check if all columns in where predicate are part of index key columns
    // TODO: Currently we allow all predicates , would it be more efficient
    // (or at least not worse) to read from index_table and not from baseTable?
    if (!indexKeyNames.containsAll(predicateColumnsList)) {
      LOG.info("Predicate column ref list has non index key column : " +
          " Cannot use index  " + index.getIndexName());
      return false;
    }

      //--------------------------------------------
      // For group by, we need to check if all keys are from index columns
      // itself. Here GB key order can be different than index columns but that does
      // not really matter for final result.
      // E.g. select c1, c2 from src group by c2, c1;
      // we can rewrite this one to:
      // select c1, c2 from src_cmpt_idx;
      if (!indexKeyNames.containsAll(gbKeyNameList)) {
        LOG.info("Group by key has some non-indexed columns, " +
            " Cannot use index  " + index.getIndexName());
        return false;
      }

       // FUTURE: See if this can be relaxed.
      // If we have agg function (currently only COUNT is supported), check if its input are
      // from index. we currently support only that.
      if (aggFuncColList.size() > 0)  {
        if (indexKeyNames.containsAll(aggFuncColList) == false) {
          LOG.info("Agg Func input is not present in index key columns. Currently " +
              "only agg func on index columns are supported by rewrite optimization" );
          return false;
        }
        // If we have count on some key, check if key is same as index key,
        if (aggFuncColList.containsAll(indexKeyNames))  {
          optimizeCount = true;
        }
      }

      if (!gbKeyNameList.containsAll(indexKeyNames))  {
        // GB key and idx key are not same, don't remove GroupBy, but still do index scan
        LOG.info("Index has some non-groupby columns, GroupBy will be"
            + " preserved by rewrite optimization but original table scan"
            + " will be replaced with index table scan." );
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
      if (queryHasGroupBy &&
          indexKeyNames.size() < gbyKeyCnt) {
        LOG.info("Group by key has some non-indexed columns, GroupBy will be"
            + " preserved by rewrite optimization" );
        removeGroupBy = false;
      }


    //Now that we are good to do this optimization, set parameters in context
    //which would be used by transformation procedure as inputs.

    //sub-query is needed only in case of optimizecount and complex gb keys?
    if(queryHasGenericUdfOnGroupbyKey == false
        && !(optimizeCount == true && removeGroupBy == false) ) {
      addTable(baseTableName, index.getIndexTableName());
    }else if(queryHasGenericUdfOnGroupbyKey == true &&
        aggFuncCnt == 1 &&
        aggFuncIsNotCount == false){
      shouldAppendSubquery = true;
      addTable(baseTableName, index.getIndexTableName());
    }else{
      LOG.info("No valid criteria met to apply rewrite." );
      return false;
    }

    return true;
  }


}
