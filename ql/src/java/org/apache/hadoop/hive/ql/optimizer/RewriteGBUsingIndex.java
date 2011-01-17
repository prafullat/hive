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
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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


public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  private final RewriteCanApplyCtx canApplyCtx = new RewriteCanApplyCtx();
  private final RewriteRemoveGroupbyCtx removeGbyCtx = new RewriteRemoveGroupbyCtx();
  private final RewriteIndexSubqueryCtx subqueryCtx = new RewriteIndexSubqueryCtx();


  ArrayList<String> tsOpToProcess = new ArrayList<String>();
  private String indexName = "";
  private Set<String> indexKeyNames = new LinkedHashSet<String>();
  private String currentTableName = null;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    try {
      hiveDb = Hive.get(parseContext.getConf());
    } catch (HiveException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    canApplyCtx.setParseContext(parseContext);
    canApplyCtx.setHiveDb(hiveDb);
    canApplyCtx.setConf(parseContext.getConf());

    removeGbyCtx.setParseContext(parseContext);
    removeGbyCtx.setHiveDb(hiveDb);

    subqueryCtx.setParseContext(parseContext);

    if(isQueryInsertToTable()){
        return parseContext;
    }else{
      if(shouldApplyOptimization()){
        LOG.debug("Rewriting Original Query.");
        indexKeyNames = canApplyCtx.getIndexKeyNames();
        indexName = canApplyCtx.getIndexName();
        rewriteOriginalQuery();
        return parseContext;
      }else{
        return null;
      }
    }

  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }


  private boolean isQueryInsertToTable(){
    QBParseInfo qbParseInfo =  parseContext.getQB().getParseInfo();
    return qbParseInfo.isInsertToTable();
  }

  private boolean shouldApplyOptimization(){
    boolean canApply = true;
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
        checkEachDAGOperator(topOp, indexTables);
      }
      canApply = canApplyCtx.checkIfOptimizationCanApply();
      if(canApply){
        canApply = canApplyCtx.isIndexUsable(indexTables);
      }
    }
    return canApply;
  }


  private void checkEachDAGOperator(Operator<? extends Serializable> topOp, List<Index> indexTables){
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

  private void rewriteOriginalQuery() throws SemanticException{
    HashMap<String, Operator<? extends Serializable>> topOpMap = parseContext.getTopOps();
    for (String topOpTableName : tsOpToProcess) {
      currentTableName = topOpTableName;
      canApplyCtx.setCurrentTableName(currentTableName);

      TableScanOperator topOp = (TableScanOperator) topOpMap.get(topOpTableName);

      if(canApplyCtx.getBoolVar(parseContext.getConf(), RewriteVars.SHOULD_APPEND_SUBQUERY)){
        subqueryCtx.setIndexKeyNames(indexKeyNames);
        subqueryCtx.setIndexName(indexName);
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

      if(canApplyCtx.getBoolVar(parseContext.getConf(), RewriteVars.REMOVE_GROUP_BY)){
        removeGbyCtx.setIndexName(indexName);
        removeGbyCtx.setOpc(parseContext.getOpParseCtx());
        removeGbyCtx.setCanApplyCtx(canApplyCtx);
        invokeRemoveGbyProc(topOp);
      }

      canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.REMOVE_GROUP_BY, false);
      canApplyCtx.setBoolVar(parseContext.getConf(), RewriteVars.SHOULD_APPEND_SUBQUERY, false);

    }

  }


  private void invokeRemoveGbyProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
       // process group-by pattern
    opRules.put(new RuleRegExp("R1", "SEL%"), RewriteRemoveGroupbyProcFactory.getReplaceIdxKeyWithSizeFuncProc());
    opRules.put(new RuleRegExp("R2", "GBY%RS%GBY%"), RewriteRemoveGroupbyProcFactory.getRemoveGroupByProc());
    opRules.put(new RuleRegExp("R3", "TS%"), RewriteRemoveGroupbyProcFactory.getReplaceTableScanProc());
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, removeGbyCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);

    /*The reason we need to do this step after walking DAg is that we need the TS operator to be
     * replaced first before replacing idx keys with size(offsets)*/
    SelectOperator selectOp = removeGbyCtx.getSelectOperator((TableScanOperator) topOp);
    removeGbyCtx.replaceIdxKeyWithSizeFunc(selectOp);

    //Getting back new parseContext and new OpParseContext after GBY-RS-GBY is removed
    parseContext = removeGbyCtx.getParseContext();
    parseContext.setOpParseCtx(removeGbyCtx.getOpc());
    LOG.info("Finished Group by Remove");

  }

  private void invokeSubquerySelectSchemaProc(Operator<? extends Serializable> topOp) throws SemanticException{


    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    //appends subquery DAG to original DAG
    opRules.put(new RuleRegExp("R1", "FS%"), RewriteIndexSubqueryProcFactory.getSubqueryFileSinkProc());
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



  private void invokeFixAllOperatorSchemasProc(Operator<? extends Serializable> topOp) throws SemanticException{


    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    //appends subquery DAG to original DAG
    opRules.put(new RuleRegExp("R1", "TS%"), RewriteIndexSubqueryProcFactory.getAppendSubqueryToOriginalQueryProc());
    opRules.put(new RuleRegExp("R2", "SEL%"), RewriteIndexSubqueryProcFactory.getNewQuerySelectSchemaProc());
    opRules.put(new RuleRegExp("R3", "FIL%"), RewriteIndexSubqueryProcFactory.getNewQueryFilterSchemaProc());
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

