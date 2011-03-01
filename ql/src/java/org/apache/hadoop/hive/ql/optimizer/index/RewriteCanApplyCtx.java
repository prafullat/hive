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
import org.apache.hadoop.hive.conf.HiveConf;
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

  private RewriteCanApplyCtx(ParseContext parseContext, HiveConf conf) {
    this.parseContext = parseContext;
    this.hiveConf = conf;
    initRewriteVars();
  }

  public static RewriteCanApplyCtx getInstance(ParseContext parseContext, HiveConf conf){
    return new RewriteCanApplyCtx(parseContext, conf);
  }

  public static enum RewriteVars {
    AGG_FUNC_CNT("hive.ql.rewrites.agg.func.cnt", 0),
    GBY_KEY_CNT("hive.ql.rewrites.gby.key.cnt", 0),
    QUERY_HAS_SORT_BY("hive.ql.rewrites.query.has.sort.by", false),
    QUERY_HAS_ORDER_BY("hive.ql.rewrites.query.has.order.by", false),
    QUERY_HAS_DISTRIBUTE_BY("hive.ql.rewrites.query.has.distribute.by", false),
    QUERY_HAS_GROUP_BY("hive.ql.rewrites.query.has.group.by", false),
    QUERY_HAS_DISTINCT("hive.ql.rewrites.query.has.distinct", false), //This still uses QBParseInfo to make decision. Needs to be changed if QB dependency is not desired.
    AGG_FUNC_IS_NOT_COUNT("hive.ql.rewrites.agg.func.is.not.count", false),
    AGG_FUNC_COLS_FETCH_EXCEPTION("hive.ql.rewrites.agg.func.cols.fetch.exception", false),
    WHR_CLAUSE_COLS_FETCH_EXCEPTION("hive.ql.rewrites.whr.clause.cols.fetch.exception", false),
    SEL_CLAUSE_COLS_FETCH_EXCEPTION("hive.ql.rewrites.sel.clause.cols.fetch.exception", false),
    GBY_KEYS_FETCH_EXCEPTION("hive.ql.rewrites.gby.keys.fetch.exception", false),
    COUNT_ON_ALL_COLS("hive.ql.rewrites.count.on.all.cols", false),
    QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY("hive.ql.rewrites.query.has.genericudf.on.groupby.key", false),
    QUERY_HAS_MULTIPLE_TABLES("hive.ql.rewrites.query.has.multiple.tables", false),
    SHOULD_APPEND_SUBQUERY("hive.ql.rewrites.should.append.subquery", false),
    REMOVE_GROUP_BY("hive.ql.rewrites.remove.group.by", false);
    ;

    public final String varname;
    public int intValue;
    public boolean boolValue;
    public final Class<?> valClass;

    //Constructors for int and boolean values
    RewriteVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.intValue = defaultIntVal;
      this.boolValue = false;
    }

    RewriteVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.intValue = -1;
      this.boolValue = defaultBoolVal;
    }

    @Override
    public String toString() {
      return varname;
    }



  }

  /*
   * Methods to set and retrieve the RewriteVars enum variables
   * */
  public int getIntVar(RewriteVars var) {
    assert (var.valClass == Integer.class);
    return var.intValue;
  }

  public void setIntVar(RewriteVars var, int val) {
    assert (var.valClass == Integer.class);
    var.intValue = val;
  }

  public boolean getBoolVar(RewriteVars var) {
    assert (var.valClass == Boolean.class);
    return var.boolValue;
  }

  public void setBoolVar(RewriteVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    var.boolValue = val;
  }

  public void initRewriteVars(){
    setIntVar(RewriteVars.AGG_FUNC_CNT,0);
    setIntVar(RewriteVars.GBY_KEY_CNT,0);
    setBoolVar(RewriteVars.QUERY_HAS_SORT_BY, false);
    setBoolVar(RewriteVars.QUERY_HAS_ORDER_BY, false);
    setBoolVar(RewriteVars.QUERY_HAS_DISTRIBUTE_BY, false);
    setBoolVar(RewriteVars.QUERY_HAS_GROUP_BY, false);
    setBoolVar(RewriteVars.QUERY_HAS_DISTINCT, false);
    setBoolVar(RewriteVars.AGG_FUNC_IS_NOT_COUNT, false);
    setBoolVar(RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION, false);
    setBoolVar(RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, false);
    setBoolVar(RewriteVars.SEL_CLAUSE_COLS_FETCH_EXCEPTION, false);
    setBoolVar(RewriteVars.GBY_KEYS_FETCH_EXCEPTION, false);
    setBoolVar(RewriteVars.COUNT_ON_ALL_COLS, false);
    setBoolVar(RewriteVars.QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY, false);
    setBoolVar(RewriteVars.QUERY_HAS_MULTIPLE_TABLES, false);
    setBoolVar(RewriteVars.SHOULD_APPEND_SUBQUERY, false);
    setBoolVar(RewriteVars.REMOVE_GROUP_BY, false);
  }




   //Data structures that are populated in the RewriteCanApplyProcFactory methods to check if the index key meets all criteria
   Set<String> selectColumnsList = new LinkedHashSet<String>();
   Set<String> predicateColumnsList = new LinkedHashSet<String>();
   Set<String> gbKeyNameList = new LinkedHashSet<String>();
   Set<String> aggFuncColList = new LinkedHashSet<String>();

   private final HiveConf hiveConf;
   private int aggFuncCnt = 0;
   private final ParseContext parseContext;
   private String baseTableName = "";

   void resetCanApplyCtx(){
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

  public HiveConf getConf() {
    return hiveConf;
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
      LOG.info("Exception in walking operator tree. Rewrite variables not populated", e);
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
    boolean removeGroupBy = true;
    boolean optimizeCount = false;

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
      if (getBoolVar(RewriteVars.QUERY_HAS_GROUP_BY) &&
          indexKeyNames.size() < getIntVar(RewriteVars.GBY_KEY_CNT)) {
        LOG.info("Group by key has some non-indexed columns, GroupBy will be"
            + " preserved by rewrite optimization" );
        removeGroupBy = false;
      }


    //Now that we are good to do this optimization, set parameters in context
    //which would be used by transformation procedure as inputs.

    //sub-query is needed only in case of optimizecount and complex gb keys?
    if(getBoolVar(RewriteVars.QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY) == false
        && !(optimizeCount == true && removeGroupBy == false) ) {
      setBoolVar(RewriteVars.REMOVE_GROUP_BY, removeGroupBy);
      addTable(baseTableName, index.getIndexTableName());
    }else if(getBoolVar(RewriteVars.QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY) == true &&
        getIntVar(RewriteVars.AGG_FUNC_CNT) == 1 &&
        getBoolVar(RewriteVars.AGG_FUNC_IS_NOT_COUNT) == false){
      setBoolVar(RewriteVars.SHOULD_APPEND_SUBQUERY, true);
      addTable(baseTableName, index.getIndexTableName());
    }else{
      LOG.info("No valid criteria met to apply rewrite." );
      return false;
    }

    return true;
  }


}
