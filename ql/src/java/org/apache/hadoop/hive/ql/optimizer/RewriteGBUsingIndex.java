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

import java.io.IOException;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  boolean SHOULD_APPEND_SUBQUERY = false;
  boolean REMOVE_GROUP_BY = false;

  private String indexName = "";
  private final Set<String> indexKeyNames = new LinkedHashSet<String>();
  private RewriteGBUsingIndexProcCtx rewriteContext = null;
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

    rewriteContext = new RewriteGBUsingIndexProcCtx();
    rewriteContext.hiveDb = hiveDb;

    CanApplyOptimization canApply = new CanApplyOptimization();

    if(canApply.isQueryInsertToTable()){
        return parseContext;
    }else{
      if(canApply.shouldApplyOptimization()){
        LOG.debug("Rewriting Original Query.");
        rewriteOriginalQuery();
        return parseContext;
      }else{
        return null;
      }
    }

  }





  private void rewriteOriginalQuery() throws SemanticException{
    HashMap<String, Operator<? extends Serializable>> topOpMap = parseContext.getTopOps();
    Iterator<String> topOpItr = topOpMap.keySet().iterator();
    ArrayList<String> tsOpToProcess = new ArrayList<String>();
    while(topOpItr.hasNext()){
      String topOpTableName = topOpItr.next();
      tsOpToProcess.add(topOpTableName);
    }

    for (String topOpTableName : tsOpToProcess) {
      currentTableName = topOpTableName;
      TableScanOperator topOp = (TableScanOperator) topOpMap.get(topOpTableName);
      if(REMOVE_GROUP_BY){
        invokeRemoveGbyProc(topOp);
      }

      if(SHOULD_APPEND_SUBQUERY){
        invokeAppendSubqueryProc(topOp);
      }
    }

    toStringTree(parseContext);
  }

  private void invokeRemoveGbyProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
       // process group-by pattern
    opRules.put(new RuleRegExp("R1", "TS%"), new RemoveGroupByProc());
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,new GroupByOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);

  }

  private void invokeAppendSubqueryProc(Operator<? extends Serializable> topOp) throws SemanticException{

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    //appends subquery DAG to original DAG
    opRules.put(new RuleRegExp("R1", "TS%"), new AppendSubqueryToOriginalDAG());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, new AppendSubqueryProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    ogw.startWalking(topNodes, null);

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


  /***********************************************************************************************/
  /* Method to print the operators in the DAG and their child operators */
  private void toStringTree(ParseContext pCtx){
    HashMap<String, Operator<? extends Serializable>> top = pCtx.getTopOps();
    Iterator<String> tabItr = top.keySet().iterator();
    while(tabItr.hasNext()){
      String tab = tabItr.next();
      LOG.info("Printing DAG for table:" + tab );
      Operator<? extends Serializable> pList = top.get(tab);
        while(pList != null){
          LOG.info("Operator = " + pList.getName() + "("
              + ((Operator<? extends Serializable>) pList).getIdentifier() + ")" );
          if(pList.getChildOperators() == null || pList.getChildOperators().size() == 0){
            pList = null;
            break;
          }else{
            List<Operator<? extends Serializable>> cList = pList.getChildOperators();
            for (Operator<? extends Serializable> operator : cList) {
              if(null != operator){
                pList = operator;
                continue;
              }
            }
          }
        }
    }
  }


  private SelectOperator getFirstSelectOperator(TableScanOperator tsOp){
    SelectOperator selOp = null;
    List<Operator<? extends Serializable>> childOps = tsOp.getChildOperators();

    while(childOps != null && childOps.size() > 0){
      for (Operator<? extends Serializable> operator : childOps) {
        if(operator != null){
          if(operator instanceof SelectOperator){
            selOp = (SelectOperator) operator;
            childOps = null;
            break;
          }else{
            childOps = operator.getChildOperators();
            continue;
          }
        }
      }
    }
    return selOp;
  }



  /************************************************CanApplyOptimization****************************************************/

  private class CanApplyOptimization {

    int NO_OF_SUBQUERIES = 0;
    boolean QUERY_HAS_JOIN = false;
    boolean TABLE_HAS_NO_INDEX = false;
    boolean QUERY_HAS_SORT_BY = false;
    boolean QUERY_HAS_ORDER_BY = false;
    boolean QUERY_HAS_DISTRIBUTE_BY = false;
    boolean QUERY_HAS_GROUP_BY = false;
    boolean QUERY_HAS_DISTINCT = false; //This still uses QBParseInfo to make decision. Needs to be changed if QB dependency is not desired.
    int AGG_FUNC_CNT = 0;
    int GBY_KEY_CNT = 0;
    boolean AGG_FUNC_IS_NOT_COUNT = false;
    boolean AGG_FUNC_COLS_FETCH_EXCEPTION = false;
    boolean WHR_CLAUSE_COLS_FETCH_EXCEPTION = false;
    boolean SEL_CLAUSE_COLS_FETCH_EXCEPTION = false;
    boolean GBY_KEYS_FETCH_EXCEPTION = false;
    boolean GBY_KEY_HAS_NON_INDEX_COLS = false;
    boolean SEL_HAS_NON_COL_REF = false;//Not sure why this is used
    boolean GBY_NOT_ON_COUNT_KEYS = false;
    boolean IDX_TBL_SEARCH_EXCEPTION = false;
    boolean QUERY_HAS_KEY_MANIP_FUNC = false;
    boolean QUERY_HAS_MULTIPLE_SUBQUERIES = false;
    boolean QUERY_HAS_MULTIPLE_TABLES = false;

    /***************************************Index Validation Variables***************************************/
    private static final String SUPPORTED_INDEX_TYPE =
      "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
    private static final String COMPACT_IDX_BUCKET_COL = "_bucketname";
    private static final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";
    private final List<String> selColRefNameList = new ArrayList<String>();
    private List<String> predColRefs = new ArrayList<String>();
    private final List<String> gbKeyNameList = new ArrayList<String>();
    private final List<List<String>> colRefAggFuncInputList = new ArrayList<List<String>>();


    /*  protected boolean shouldApplyOptimization()  {
        QB inputQb = parseContext.getQB();
        boolean retValue = true;
        for (String subqueryAlias : inputQb.getSubqAliases()) {
          QB childSubQueryQb = inputQb.getSubqForAlias(subqueryAlias).getQB();
          //retValue |= checkSingleDAG(childSubQueryQb) ;
        }
        //retValue |= checkSingleDAG(inputQb) ;
        return retValue;
      }
    */

      private boolean isQueryInsertToTable(){
        QBParseInfo qbParseInfo =  parseContext.getQB().getParseInfo();
        return qbParseInfo.isInsertToTable();
      }

      private String getName() {
        return "RewriteGBUsingIndex";
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
          QUERY_HAS_MULTIPLE_TABLES = true;
          return false;
        }

        Iterator<TableScanOperator> topOpItr = topToTable.keySet().iterator();
        while(topOpItr.hasNext()){
          AGG_FUNC_CNT = 0;
          GBY_KEY_CNT = 0;
          TableScanOperator topOp = topOpItr.next();
          Table table = topToTable.get(topOp);
          currentTableName = table.getTableName();
          canApply =  checkSingleDAG(topOp);
          if(!canApply) {
            break;
          }
        }
        canApply = checkIfOptimizationCanApply();
        return canApply;
      }



    private boolean checkSingleDAG(TableScanOperator topOp) {
      boolean canApply = true;
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
          canApply = DAGTraversal(topOp.getChildOperators(), indexTables);
        }
      }
      return canApply;
    }

    private boolean DAGTraversal(List<Operator<? extends Serializable>> topOpChildrenList, List<Index> indexTables){
      boolean result = true;
      List<Operator<? extends Serializable>> childrenList = topOpChildrenList;
      while(childrenList != null && childrenList.size() > 0){
        for (Operator<? extends Serializable> operator : childrenList) {
          if(null != operator){
            if(operator instanceof JoinOperator){
              //QUERY_HAS_JOIN = true;
              //return false;
            }else if(operator instanceof ExtractOperator){
              result = canApplyOnExtractOperator(operator);
            }else if(operator instanceof GroupByOperator){
              result = canApplyOnGroupByOperator(operator);
            }else if(operator instanceof FilterOperator){
              result = canApplyOnFilterOperator(operator);
            }else if(operator instanceof SelectOperator){
              result = canApplyOnSelectOperator(operator);
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

      if(result){
          result = isIndexUsable(indexTables);
      }
      return result;
    }


    private boolean canApplyOnGroupByOperator(Operator<? extends Serializable> operator){
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
               ArrayList<ExprNodeDesc> para = aggregationDesc.getParameters();
                if(para == null){
                  AGG_FUNC_COLS_FETCH_EXCEPTION =  true;
                  return false;
                }else if(para.size() == 0){
                  LOG.info("count(*) case");
                  GBY_NOT_ON_COUNT_KEYS = true;
                  return false;
                }else{
                  for(int i=0; i< para.size(); i++){
                    ExprNodeDesc end = para.get(i);
                    if(end instanceof ExprNodeConstantDesc){
                      LOG.info("count(const) case");
                      //selColRefNameList.add(((ExprNodeConstantDesc) end).getValue().toString());
                      //GBY_NOT_ON_COUNT_KEYS = true;
                      //return false;
                    }else if(end instanceof ExprNodeColumnDesc){
                      selColRefNameList.add(((ExprNodeColumnDesc) end).getColumn());
                      colRefAggFuncInputList.add(para.get(i).getCols());
                    }
                  }
                }
              }
            }
        }else{
          QBParseInfo qbParseInfo =  parseContext.getQB().getParseInfo();
          Set<String> clauseNameSet = qbParseInfo.getClauseNames();
          if (clauseNameSet.size() != 1) {
            return false;
          }
          Iterator<String> clauseNameIter = clauseNameSet.iterator();
          String clauseName = clauseNameIter.next();
          ASTNode rootSelExpr = qbParseInfo.getSelForClause(clauseName);
          boolean isDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
          if(isDistinct) {
            QUERY_HAS_DISTINCT = true;
          }
        }
        ArrayList<ExprNodeDesc> keyList = conf.getKeys();
        if(keyList == null || keyList.size() == 0){
          GBY_KEYS_FETCH_EXCEPTION = true;
          return false;
        }
        GBY_KEY_CNT = keyList.size();
        for (ExprNodeDesc exprNodeDesc : keyList) {
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            gbKeyNameList.addAll(exprNodeDesc.getCols());
            selColRefNameList.add(((ExprNodeColumnDesc) exprNodeDesc).getColumn());
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc endfg = (ExprNodeGenericFuncDesc)exprNodeDesc;
            List<ExprNodeDesc> childExprs = endfg.getChildExprs();
            for (ExprNodeDesc end : childExprs) {
              if(end instanceof ExprNodeColumnDesc){
                QUERY_HAS_KEY_MANIP_FUNC = true;
                gbKeyNameList.addAll(exprNodeDesc.getCols());
                selColRefNameList.add(((ExprNodeColumnDesc) end).getColumn());
              }
            }
          }
        }

      }
      return true;
    }

    private boolean canApplyOnExtractOperator(Operator<? extends Serializable> operator){
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

    private boolean canApplyOnFilterOperator(Operator<? extends Serializable> operator){
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

    private boolean canApplyOnSelectOperator(Operator<? extends Serializable> operator){
      List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
      Operator<? extends Serializable> child = childrenList.get(0);
      if(child instanceof GroupByOperator){
        return true;
      }else{
        SelectDesc conf = (SelectDesc)operator.getConf();
        ArrayList<ExprNodeDesc> selColList = conf.getColList();
        if(selColList == null || selColList.size() == 0){
          SEL_CLAUSE_COLS_FETCH_EXCEPTION = true;
          return false;
        }else{
          if(!QUERY_HAS_GROUP_BY){
            for (ExprNodeDesc exprNodeDesc : selColList) {
              if(exprNodeDesc instanceof ExprNodeColumnDesc){
                selColRefNameList.addAll(exprNodeDesc.getCols());
              }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
                ExprNodeGenericFuncDesc endfg = (ExprNodeGenericFuncDesc)exprNodeDesc;
                List<ExprNodeDesc> childExprs = endfg.getChildExprs();
                for (ExprNodeDesc end : childExprs) {
                  if(end instanceof ExprNodeColumnDesc){
                    selColRefNameList.addAll(exprNodeDesc.getCols());
                  }
                }
              }
            }

          }

        }
    }

      return true;
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



    private boolean isIndexUsable(List<Index> indexTables){
      Index index = null;
      Hive hiveInstance = hiveDb;

      // This code block iterates over indexes on the table and picks up the
      // first index that satisfies the rewrite criteria.
      for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
        boolean removeGroupBy = true;
        boolean optimizeCount = false;
        List<String> idxKeyColsNames = new ArrayList<String>();

        index = indexTables.get(idxCtr);
        indexName = index.getIndexTableName();
        //Getting index key columns
        StorageDescriptor sd = index.getSd();
        List<FieldSchema> idxColList = sd.getCols();
        for (FieldSchema fieldSchema : idxColList) {
          idxKeyColsNames.add(fieldSchema.getName());
          indexKeyNames.add(fieldSchema.getName());
        }



        // Check that the index schema is as expected. This code block should
        // catch problems of this rewrite breaking when the CompactIndexHandler
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
          IDX_TBL_SEARCH_EXCEPTION = true;
          return false;
        }
        assert(idxTblColNames.contains(COMPACT_IDX_BUCKET_COL));
        assert(idxTblColNames.contains(COMPACT_IDX_OFFSETS_ARRAY_COL));
        assert(idxTblColNames.size() == idxKeyColsNames.size() + 2);

        //--------------------------------------------
        //Check if all columns in select list are part of index key columns
        if (!idxKeyColsNames.containsAll(selColRefNameList)) {
          LOG.info("Select list has non index key column : " +
              " Cannot use this index  " + index.getIndexName());
          continue;
        }

        // We need to check if all columns from index appear in select list only
        // in case of DISTINCT queries, In case group by queries, it is okay as long
        // as all columns from index appear in group-by-key list.
        if (QUERY_HAS_DISTINCT) {
          // Check if all columns from index are part of select list too
          if (!selColRefNameList.containsAll(idxKeyColsNames))  {
            LOG.info("Index has non select list columns " +
                " Cannot use this index  " + index.getIndexName());
            continue;
          }
        }

        //--------------------------------------------
        // Check if all columns in where predicate are part of index key columns
        // TODO: Currently we allow all predicates , would it be more efficient
        // (or at least not worse) to read from index_table and not from baseTable?
        if (!idxKeyColsNames.containsAll(predColRefs)) {
          LOG.info("Predicate column ref list has non index key column : " +
              " Cannot use this index  " + index.getIndexName());
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
          if (!idxKeyColsNames.containsAll(gbKeyNameList)) {
            GBY_KEY_HAS_NON_INDEX_COLS = true;
            return false;
          }

          if (!gbKeyNameList.containsAll(idxKeyColsNames))  {
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
              idxKeyColsNames.size() < GBY_KEY_CNT) {
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
        //which would be used by transformation procedure as inputs.

        //sub-query is needed only in case of optimizecount and complex gb keys?
        if(QUERY_HAS_KEY_MANIP_FUNC == false && !(optimizeCount == true && removeGroupBy == false) ) {
          REMOVE_GROUP_BY = removeGroupBy;
          rewriteContext.addTable(currentTableName, index.getIndexTableName());
        }else{
          SHOULD_APPEND_SUBQUERY = true;
        }

      }

      return true;

    }


    private boolean checkIfOptimizationCanApply(){
      boolean canApply = false;
      if (QUERY_HAS_MULTIPLE_TABLES) {
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
        LOG.info("Table " + currentTableName + " does not have compact index. " +
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
      if(AGG_FUNC_CNT > 1 ){
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
  /*    if(QUERY_HAS_DISTINCT){
        LOG.info("Select-list has distinct. " +
            "Cannot apply the rewrite " + getName() + " optimization" );
        return canApply;
      }//12
  */    if(SEL_HAS_NON_COL_REF){
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


  }

  /******************************************RewriteGBUsingIndexProcCtx*****************************************************/

  public class RewriteGBUsingIndexProcCtx implements NodeProcessorCtx {
    private Hive hiveDb;

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


  /********************************************SubqueryParseContextGenerator***************************************************/
/*
 *Class to instantialte SemanticAnalyzer and create the operator DAG for subquery
 *Method generateDAGForSubquery() returns the new ParseContext object for the subquery DAG
 */
  public class SubqueryParseContextGenerator {

    public SubqueryParseContextGenerator() {
    }

    private ParseContext generateDAGForSubquery(String command){
      HiveConf conf = parseContext.getConf();
      Context ctx;
      ParseContext subPCtx = null;
      try {
        ctx = new Context(conf);
      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      doSemanticAnalysis(ctx, sem, tree);

      subPCtx = ((SemanticAnalyzer) sem).getParseContext();
      toStringTree(subPCtx);

      LOG.info("Sub-query Semantic Analysis Completed");
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (SemanticException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return subPCtx;

    }

    @SuppressWarnings("unchecked")
    private Operator<Serializable> doSemanticAnalysis(Context ctx, BaseSemanticAnalyzer sem, ASTNode ast) throws SemanticException {

      if(sem instanceof SemanticAnalyzer){
        QB qb = new QB(null, null, false);
        ASTNode child = ast;
        ParseContext subPCtx = ((SemanticAnalyzer) sem).getParseContext();
        subPCtx.setContext(ctx);
        ((SemanticAnalyzer) sem).init(subPCtx);


        LOG.info("Starting Sub-query Semantic Analysis");
        ((SemanticAnalyzer) sem).doPhase1(child, qb, ((SemanticAnalyzer) sem).initPhase1Ctx());
        LOG.info("Completed phase 1 of Sub-query Semantic Analysis");

        ((SemanticAnalyzer) sem).getMetaData(qb);
        LOG.info("Completed getting MetaData in Sub-query Semantic Analysis");

        LOG.info("Sub-query Abstract syntax tree: " + ast.toStringTree());
        Operator<Serializable> sinkOp = ((SemanticAnalyzer) sem).genPlan(qb);

        //LOG.info("Processing for Sub-query = " + sinkOp.getName() + "(" + ((Operator<Serializable>) sinkOp).getIdentifier() + ")");
        LOG.info("Sub-query Completed plan generation");
         return sinkOp;

      } else {
        return null;
      }

    }

  }


  /**********************************************RemoveGroupByProc*************************************************/
  public class RemoveGroupByProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      // GBY,RS,GBY... (top to bottom)
      TableScanOperator op = (TableScanOperator) nd;

      removeGroupBy(op);
      replaceOrigTableWithIndexTable(op);

      SelectOperator selectOp = getFirstSelectOperator(op);
      processGenericUDF(selectOp);
      return null;
    }


    private void removeGroupBy(TableScanOperator tsOp){
      List<Operator<? extends Serializable>>  newParentList = new ArrayList<Operator<? extends Serializable>>();
      List<Operator<? extends Serializable>>  newChildrenList = new ArrayList<Operator<? extends Serializable>>();
      List<Operator<? extends Serializable>>  currChildren = new ArrayList<Operator<? extends Serializable>>();
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = parseContext.getOpParseCtx();

      currChildren = tsOp.getChildOperators();
      while(currChildren != null && currChildren.size() > 0){
        for (Operator<? extends Serializable> operator : currChildren) {
          if(null != operator){

            if(operator instanceof GroupByOperator){
              if(!parseContext.getGroupOpToInputTables().containsKey(operator)){
                newChildrenList = operator.getChildOperators();
                newParentList.get(0).setChildOperators(newChildrenList);
                newChildrenList.get(0).setParentOperators(newParentList);
              }else{
                parseContext.getGroupOpToInputTables().remove(operator);
              }
              opc.remove(operator);
            }else if(operator instanceof ReduceSinkOperator){
              opc.remove(operator);
            }else if(operator instanceof SelectOperator){
              List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
              Operator<? extends Serializable> child = childrenList.get(0);
              if(child instanceof GroupByOperator){
                newParentList = operator.getParentOperators();
                opc.remove(operator);
              }else{
                HashMap<String, String> internalToAlias = new LinkedHashMap<String, String>();
                RowSchema rs = operator.getSchema();
                ArrayList<ColumnInfo> sign = rs.getSignature();
                for (ColumnInfo columnInfo : sign) {
                  String alias = null;
                  String internalName = null;
                  alias = columnInfo.getAlias();
                  internalName = columnInfo.getInternalName();
                  internalToAlias.put(internalName, alias);
                }

                Map<String, ExprNodeDesc> origColExprMap = operator.getColumnExprMap();
                Map<String, ExprNodeDesc> newColExprMap = new LinkedHashMap<String, ExprNodeDesc>();

                Set<String> internalNamesList = origColExprMap.keySet();
                for (String internal : internalNamesList) {
                  ExprNodeDesc end = origColExprMap.get(internal).clone();
                  if(end instanceof ExprNodeColumnDesc){
                    ((ExprNodeColumnDesc) end).setColumn(internalToAlias.get(internal));
                  }
                  newColExprMap.put(internal, end);
                }
                operator.setColumnExprMap(newColExprMap);


                SelectDesc selDesc = (SelectDesc) operator.getConf();
                ArrayList<ExprNodeDesc> oldColList = selDesc.getColList();
                ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();

                for (int i = 0; i < oldColList.size(); i++) {
                  ExprNodeDesc exprNodeDesc = oldColList.get(i).clone();
                  if(exprNodeDesc instanceof ExprNodeColumnDesc){
                    String internal = ((ExprNodeColumnDesc)exprNodeDesc).getColumn();
                    ((ExprNodeColumnDesc) exprNodeDesc).setColumn(internalToAlias.get(internal));
                    newColList.add(exprNodeDesc);
                  }
                }
                selDesc.setColList(newColList);

             }

            }else if(operator instanceof FileSinkOperator){
              currChildren = null;
              break;
            }
            currChildren = operator.getChildOperators();
            continue;
          }
        }
      }
//
    }

    private void replaceOrigTableWithIndexTable(TableScanOperator scanOperator) throws SemanticException{
      HashMap<TableScanOperator, Table>  topToTable =
        parseContext.getTopToTable();

      String baseTableName = topToTable.get(scanOperator).getTableName();
      if( rewriteContext.findBaseTable(baseTableName) == null ) {
        LOG.debug("No mapping found for original table and index table name");
      }

      //Get the lineage information corresponding to this
      //and modify it ?
      TableScanDesc indexTableScanDesc = new TableScanDesc();
      indexTableScanDesc.setGatherStats(false);

      String tableName = rewriteContext.findBaseTable(baseTableName);

      tableSpec ts = new tableSpec(rewriteContext.hiveDb,
          parseContext.getConf(),
          tableName
      );
      String k = tableName + Path.SEPARATOR;
      indexTableScanDesc.setStatsAggPrefix(k);
      scanOperator.setConf(indexTableScanDesc);

      topToTable.clear();
      parseContext.getTopOps().clear();

      //Scan operator now points to other table
      scanOperator.setAlias(tableName);
      topToTable.put(scanOperator, ts.tableHandle);
      parseContext.setTopToTable(topToTable);

      OpParseContext operatorContext =
        parseContext.getOpParseCtx().get(scanOperator);
      RowResolver rr = new RowResolver();
      parseContext.getOpParseCtx().remove(scanOperator);



      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) ts.tableHandle.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
        .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          rr.put(tableName, fields.get(i).getFieldName(), new ColumnInfo(fields
              .get(i).getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), tableName, false));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      //Set row resolver for new table
      operatorContext.setRowResolver(rr);
      parseContext.getOpParseCtx().put(scanOperator, operatorContext);
      parseContext.getTopOps().put(tableName, scanOperator);

    }


    private ASTNode getFuncNode(ASTNode root){
      ASTNode func = null;
      ArrayList<Node> cList = root.getChildren();
      while(cList != null && cList.size() > 0){
        for (Node node : cList) {
          if(null != node){
            ASTNode curr = (ASTNode)node;
            if(curr.getType() == HiveParser.TOK_FUNCTION){
              func = curr;
              cList = null;
              break;
            }else{
              cList = curr.getChildren();
              continue;
            }
          }
        }
      }
      return func;
    }

    private void processGenericUDF(Operator<? extends Serializable> selOperator) throws SemanticException{

      HiveConf conf = parseContext.getConf();
      Context ctx = null;
      ASTNode tree = null;
      BaseSemanticAnalyzer sem = null;
      String newSelCommand = "select size(`_offsets`) from " + indexName;
      try {
      ctx = new Context(conf);
      ParseDriver pd = new ParseDriver();
      tree = pd.parse(newSelCommand, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);
      sem = SemanticAnalyzerFactory.get(conf, tree);

      } catch (ParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (SemanticException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }


      ASTNode funcNode = getFuncNode(tree);
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opCtxMap =
        parseContext.getOpParseCtx();
      List<Operator<? extends Serializable>> parentList = selOperator.getParentOperators();
      Operator<? extends Serializable> tsOp = null;
      while(parentList != null && parentList.size() > 0){
        for (Operator<? extends Serializable> operator : parentList) {
          if(operator != null){
            if(operator instanceof TableScanOperator){
              tsOp = operator;
              parentList = null;
              break;
            }else{
              parentList = operator.getParentOperators();
              continue;
            }
          }
        }
      }


      OpParseContext tsCtx = opCtxMap.get(tsOp);
      ExprNodeDesc exprNode = ((SemanticAnalyzer) sem).genExprNodeDesc(funcNode, tsCtx.getRowResolver());
      String aggFuncCol = null;

      if(exprNode instanceof ExprNodeGenericFuncDesc){
        List<ExprNodeDesc> exprList = ((ExprNodeGenericFuncDesc) exprNode).getChildExprs();
        for (ExprNodeDesc exprNodeDesc : exprList) {
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            aggFuncCol = ((ExprNodeColumnDesc) exprNodeDesc).getColumn();
          }
        }
      }

      SelectDesc selDesc = (SelectDesc) selOperator.getConf();


      RowSchema rs = selOperator.getSchema();
      ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
      ArrayList<ColumnInfo> sign = rs.getSignature();
      for (ColumnInfo columnInfo : sign) {
        String alias = columnInfo.getAlias();
        if(alias.contains("_c")){
          columnInfo.setAlias(aggFuncCol);
        }
        newRS.add(columnInfo);
      }
      selOperator.getSchema().setSignature(newRS);

      ArrayList<ExprNodeDesc> colList = selDesc.getColList();
      int i = 0;
      for (; i< colList.size(); i++) {
        ExprNodeDesc exprNodeDesc = colList.get(i);
        if(exprNodeDesc instanceof ExprNodeColumnDesc){
          if(((ExprNodeColumnDesc) exprNodeDesc).getColumn().contains("_c")){
            colList.set(i, exprNode);
            break;
          }
        }
      }

      selDesc.setColList(colList);

      Map<String, ExprNodeDesc> origColExprMap = selOperator.getColumnExprMap();
      Map<String, ExprNodeDesc> newColExprMap = new LinkedHashMap<String, ExprNodeDesc>();

      Set<String> internalNamesList = origColExprMap.keySet();
      for (String internal : internalNamesList) {
        ExprNodeDesc end = origColExprMap.get(internal).clone();
        if(end instanceof ExprNodeColumnDesc){
          if(((ExprNodeColumnDesc) end).getColumn().contains("_c")){
            newColExprMap.put(internal, exprNode);
          }else{
            newColExprMap.put(internal, end);
          }
        }else{
          newColExprMap.put(internal, end);
        }
      }
      selOperator.setColumnExprMap(newColExprMap);

    }

  }

  public class GroupByOptProcCtx implements NodeProcessorCtx {
  }



  /********************************************AppendSubqueryToOriginalDAG***************************************************/

  private class AppendSubqueryToOriginalDAG implements NodeProcessor {
    private Map<String, ExprNodeDesc> tsSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
    private Map<String, ExprNodeDesc> gbySelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
    private final Map<String, ExprNodeDesc> fsSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();

    private final ArrayList<ExprNodeDesc> gbySelColList = new ArrayList<ExprNodeDesc>();
    private final ArrayList<ExprNodeDesc> fsSelColList = new ArrayList<ExprNodeDesc>();

    ParseContext subqueryPctx = null;
    ParseContext newDAGCtx = null;

    //Initialise all data structures required to copy RowResolver and RowSchema from subquery DAG to original DAG operators
    ArrayList<String> newOutputCols = new ArrayList<String>();
    Map<String, ExprNodeDesc> newColExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
    ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
    RowResolver newRR = new RowResolver();
    private final Map<String, String> aliasToInternal = new LinkedHashMap<String, String>();

    // Get the parentOperators List for FileSinkOperator. We need this later to set the parentOperators for original DAG operator
    List<Operator<? extends Serializable>> subqFSParentList = null;
    //We need the reference to this SelectOperator so that the original DAG can be appended here
    Operator<? extends Serializable> subqSelectOp = null;

    //OperatorToParseContexts for original DAG and subquery DAG
    LinkedHashMap<Operator<? extends Serializable>, OpParseContext> origOpOldOpc = null;
    LinkedHashMap<Operator<? extends Serializable>, OpParseContext> subqOpOldOpc = null;


    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      TableScanOperator tsOp = (TableScanOperator)nd;

      String selKeys = "";
      for (String key : indexKeyNames) {
        selKeys += key + ",";
      }

      String subqueryCommand = "select " + selKeys + " size(`_offsets`) as CNT from " + indexName;
      createSubqueryDAG(subqueryCommand);
      getSubquerySelectOpSchema();

      appendSubquery(tsOp);
      fixOperatorSchema((TableScanOperator)nd);
      return null;
    }


    private void createSubqueryDAG(String subqueryCommand){
      origOpOldOpc = parseContext.getOpParseCtx();
      subqueryPctx = (new SubqueryParseContextGenerator()).generateDAGForSubquery(subqueryCommand);
      subqOpOldOpc = subqueryPctx.getOpParseCtx();

    }

    private void getSubquerySelectOpSchema(){
      HashMap<TableScanOperator, Table> topOpMap = subqueryPctx.getTopToTable();
      Iterator<TableScanOperator> topOpItr = topOpMap.keySet().iterator();
      TableScanOperator tsOp = null;
      if(topOpItr.hasNext()){
        tsOp = topOpItr.next();
      }

      SelectOperator selOp = getFirstSelectOperator(tsOp);
      FileSinkOperator fs = (FileSinkOperator) selOp.getChildOperators().get(0);
      subqFSParentList = fs.getParentOperators();
      subqSelectOp =  selOp;

      RowResolver oldRR = subqOpOldOpc.get(selOp).getRowResolver();
      SelectDesc oldConf = (SelectDesc) selOp.getConf();
      Map<String, ExprNodeDesc> oldColumnExprMap = selOp.getColumnExprMap();
      ArrayList<ExprNodeDesc> oldColList = oldConf.getColList();

      String internalName = null;
      for(int i=0; i < oldConf.getOutputColumnNames().size(); i++){
        internalName = oldConf.getOutputColumnNames().get(i);
        //Fetch all output columns (required by SelectOperators in original DAG)
        newOutputCols.add(new String(internalName));

        if(oldColumnExprMap != null){
          ExprNodeDesc end = oldColumnExprMap.get(internalName);
          if(end instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc oldDesc = (ExprNodeColumnDesc)end ;
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) oldDesc.clone();
            newDesc.setColumn(internalName);
            //Fetch columnExprMap (required by SelectOperator and FilterOperator in original DAG)
            newColExprMap.put(internalName, newDesc);
          }else if(end instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)end ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) {
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              newColExprMap.put(internalName, newDesc);
            }
          }
        }
        if(oldColList != null){
          ExprNodeDesc exprNodeDesc = oldColList.get(i);
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) exprNodeDesc.clone();
            newDesc.setColumn(internalName);
            //Fetch colList (required by SelectOperators in original DAG)
            newColList.add(newDesc);
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)exprNodeDesc ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) {
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              newColList.add(newDesc);
            }

          }
        }


      }

      //We need to set the alias for the new index table subquery
      for (int i = 0; i < newOutputCols.size(); i++) {
        internalName = newOutputCols.get(i);
        String[] nm = oldRR.reverseLookup(internalName);
        ColumnInfo col;
        try {
          col = oldRR.get(nm[0], nm[1]);
          if(nm[0] == null){
            nm[0] = "v1";
          }
          // Fetch RowResolver and RowSchema (required by SelectOperator and FilterOperator in original DAG)
          newRR.put(nm[0], nm[1], col);
          newRS.add(col);
        } catch (SemanticException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }


      subqOpOldOpc.remove(fs);
      subqueryPctx.setOpParseCtx(subqOpOldOpc);

    }


    @SuppressWarnings("unchecked")
    private void appendSubquery(TableScanOperator origOp){

     List<Operator<? extends Serializable>> origChildrenList = origOp.getChildOperators();

      /*
       * origChildrenList has the child operators for the TableScanOperator of the original DAG
       * We need to get rid of the TS operator of original DAG and append rest of the tree to the sub-query operator DAG
       * This code sets the parentOperators of first operator in origChildrenList to subqFSParentList
       * subqFSParentList contains the parentOperators list of the FileSinkOperator of the sub-query operator DAG
       *
       * subqLastOp is the last SelectOperator of sub-query DAG. The rest of the original operator DAG needs to be appended here
       * Hence, set the subqLastOp's child operators to be origChildrenList
       */
      if(origChildrenList != null && origChildrenList.size() > 0){
        origChildrenList.get(0).setParentOperators(subqFSParentList);
      }
      if(subqSelectOp != null){
        subqSelectOp.setChildOperators(origChildrenList);
      }

      //Loop through the rest of the original operator DAG and set appropriate column informations, RowResolvers and RowSchema
      List<Operator<? extends Serializable>> origList = origChildrenList;
        while(origList != null && origList.size() > 0){
          for (Operator<? extends Serializable> operator : origList) {
            if(null != operator){

              //Copy colList and outputColumns for SelectOperator from sub-query DAG SelectOperator
              if(operator instanceof SelectOperator) {
                //Set columnExprMap + RowSchema + RowResolver - required by all operators
                Operator<? extends Serializable> selChild = operator.getChildOperators().iterator().next();
                if( (!(selChild instanceof FileSinkOperator)) && (!(selChild instanceof ReduceSinkOperator))){
                  operator.setColumnExprMap(newColExprMap);
                  origOpOldOpc.get(operator).setRowResolver(newRR);
                  operator.getSchema().setSignature(newRS);
                  SelectDesc conf = (SelectDesc) operator.getConf();
                  conf.setColList(newColList);
                  conf.setOutputColumnNames(newOutputCols);

                  ArrayList<ColumnInfo> schemaSign = operator.getSchema().getSignature();
                  for (ColumnInfo columnInfo : schemaSign) {
                    String internal = columnInfo.getInternalName();
                    String alias = columnInfo.getAlias();
                    aliasToInternal.put(alias, internal);
                  }

                }

              //Copy output columns of SelectOperator from sub-query DAG to predicates of FilterOperator
              }else if(operator instanceof FilterOperator) {
                operator.getSchema().setSignature(newRS);
                origOpOldOpc.get(operator).setRowResolver(newRR);
             }else if(operator instanceof FileSinkOperator) {
               RowResolver rr = origOpOldOpc.get(operator).getRowResolver();
               OpParseContext ctx = new OpParseContext(rr);
               subqOpOldOpc.put(operator, ctx);

                origList = null;
                break;
              }

              //We need to add operator context in the sub-query OpToParseContext Map (subqOpOldOpc) for those operators
              // in original DAG that are appended to sub-query DAG
              RowResolver rr = origOpOldOpc.get(operator).getRowResolver();
              OpParseContext ctx = new OpParseContext(rr);
              subqOpOldOpc.put(operator, ctx);

              //Continue Looping until we reach end of original DAG i.e. FileSinkOperator
              origList = operator.getChildOperators();
              continue;
            }
          }
        }

        /*
         * The operator DAG plan is generated in the order FROM-WHERE-GROUPBY-ORDERBY-SELECT
         * We have appended the original operator DAG at the end of the sub-query operator DAG
         *      as the sub-query will always be a part of FROM processing
         *
         * Now we need to insert the final sub-query+original DAG to the original ParseContext
         * parseContext.setOpParseCtx(subqOpOldOpc) sets the subqOpOldOpc OpToParseContext map to the original context
         * parseContext.setTopOps(subqTopMap) sets the topOps map to contain the sub-query topOps map
         * parseContext.setTopToTable(newTopToTable) sets the original topToTable to contain sub-query top TableScanOperator
         */
        //HashMap<TableScanOperator, Table> newTopToTable = (HashMap<TableScanOperator, Table>) parseContext.getTopToTable().clone();
        //newTopToTable.remove(origOp);

        HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryPctx.getTopOps();
        Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
        String subqTab = subqTabItr.next();
        Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);

        Table tbl = subqueryPctx.getTopToTable().get(subqOp);
        //newTopToTable.put((TableScanOperator) subqOp, tbl);
        parseContext.getTopToTable().remove(origOp);
        parseContext.getTopToTable().put((TableScanOperator) subqOp, tbl);

        String tabAlias = "";
        if(currentTableName.contains(":")){
          String[] tabToAlias = currentTableName.split(":");
          if(tabToAlias.length > 1){
            tabAlias = tabToAlias[0] + ":";
          }
        }

        parseContext.getTopOps().remove(currentTableName);
        parseContext.getTopOps().put(tabAlias + subqTab, subqOp);
        //parseContext.setTopOps(subqTopMap);
        parseContext.getOpParseCtx().remove(origOp);
        parseContext.getOpParseCtx().putAll(subqOpOldOpc);
        //parseContext.setOpParseCtx(subqOpOldOpc);

    }


    private void fixOperatorSchema(TableScanOperator tsOp) throws SemanticException{
      List<Operator<? extends Serializable>> childOps = tsOp.getChildOperators();

      while(childOps != null && childOps.size() > 0){
        for (Operator<? extends Serializable> operator : childOps) {
          if(operator != null){
            if(operator instanceof SelectOperator){
              processSelOp(operator);
            }else if(operator instanceof GroupByOperator && (parseContext.getGroupOpToInputTables().containsKey(operator))){
              processGenericUDAF((GroupByOperator) operator);
            }else if(operator instanceof FilterOperator) {
              processFilOp(operator);
            }else if(operator instanceof FileSinkOperator){
              childOps = null;
              break;
            }
            childOps = operator.getChildOperators();
            continue;
          }
        }
      }



    }

    private void processFilOp(Operator<? extends Serializable> op){
      FilterOperator filOp = (FilterOperator)op;
      FilterDesc conf = filOp.getConf();
      ExprNodeDesc exprNodeDesc = conf.getPredicate();
      setFilterPredicateCol(exprNodeDesc);
      conf.setPredicate(exprNodeDesc);
      op = filOp;
    }

    private void setFilterPredicateCol(ExprNodeDesc exprNodeDesc){
      if(exprNodeDesc instanceof ExprNodeColumnDesc){
        ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
        String col = encd.getColumn();
        if(indexKeyNames.contains(col)){
          encd.setColumn(aliasToInternal.get(col));
        }
        exprNodeDesc = encd;
      }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
        ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc)exprNodeDesc;
        List<ExprNodeDesc> colExprs = engfd.getChildExprs();
        for (ExprNodeDesc colExpr : colExprs) {
          setFilterPredicateCol(colExpr);
        }
      }

    }

     private void processSelOp(Operator<? extends Serializable> selOperator){

      List<Operator<? extends Serializable>> parentOps = selOperator.getParentOperators();
      Operator<? extends Serializable> parentOp = parentOps.iterator().next();
      List<Operator<? extends Serializable>> childOps = selOperator.getChildOperators();
      Operator<? extends Serializable> childOp = childOps.iterator().next();
      SelectDesc selDesc = (SelectDesc) selOperator.getConf();

      if(parentOp instanceof TableScanOperator){
        tsSelColExprMap = selOperator.getColumnExprMap();
      }else if (childOp instanceof GroupByOperator){
        gbySelColExprMap = selOperator.getColumnExprMap();
        /* colExprMap */
        Set<String> internalNamesList = gbySelColExprMap.keySet();
        for (String internal : internalNamesList) {
          ExprNodeDesc end = gbySelColExprMap.get(internal).clone();
          if(end instanceof ExprNodeGenericFuncDesc){
            List<ExprNodeDesc> colExprs = ((ExprNodeGenericFuncDesc)end).getChildExprs();
            for (ExprNodeDesc colExpr : colExprs) {
              if(colExpr instanceof ExprNodeColumnDesc){
                fsSelColExprMap.put(internal, colExpr);
                gbySelColList.add(colExpr);
                fsSelColList.add(colExpr);
              }
            }

          }else if(end instanceof ExprNodeColumnDesc){
            fsSelColExprMap.put(internal, end);
            gbySelColList.add(end);
            fsSelColList.add(end);
          }
        }

        selOperator.setColumnExprMap(tsSelColExprMap);
        selDesc.setColList(gbySelColList);
      }
    }

    private void processGenericUDAF(GroupByOperator oldGbyOperator) throws SemanticException{
      String table = currentTableName;
      if(table.contains(":")){
        String[] aliasAndTab = table.split(":");
        table = aliasAndTab[1];
      }
      String selReplacementCommand = "";
      if(indexKeyNames.iterator().hasNext()){
        selReplacementCommand = "select sum(" + indexKeyNames.iterator().next() + ") as TOTAL from " + table
        + " group by " + indexKeyNames.iterator().next() + " ";
      }
      SubqueryParseContextGenerator newDAGCreator = new SubqueryParseContextGenerator();
      newDAGCtx = newDAGCreator.generateDAGForSubquery(selReplacementCommand);
      Map<GroupByOperator, Set<String>> newGbyOpMap = newDAGCtx.getGroupOpToInputTables();
      GroupByOperator newGbyOperator = newGbyOpMap.keySet().iterator().next();

      GroupByDesc oldConf = oldGbyOperator.getConf();
      ArrayList<AggregationDesc> oldAggrList = oldConf.getAggregators();
      if(oldAggrList != null && oldAggrList.size() > 0){
        for (AggregationDesc aggregationDesc : oldAggrList) {
          if(aggregationDesc != null && aggregationDesc.getGenericUDAFName().equals("count")){
            oldAggrList.remove(aggregationDesc);
            break;
          }

        }
      }

      GenericUDAFEvaluator eval = null;

      GroupByDesc newConf = newGbyOperator.getConf();
      ArrayList<AggregationDesc> newAggrList = newConf.getAggregators();
      if(newAggrList != null && newAggrList.size() > 0){
        for (AggregationDesc aggregationDesc : newAggrList) {
          eval = aggregationDesc.getGenericUDAFEvaluator();
          ArrayList<ExprNodeDesc> paraList = aggregationDesc.getParameters();
          for (int i=0; i< paraList.size(); i++) {
            ExprNodeDesc exprNodeDesc = paraList.get(i);
            if(exprNodeDesc instanceof ExprNodeColumnDesc){
              ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
              String col = "cnt";
              if(aliasToInternal.containsKey(col)){
                encd.setColumn(aliasToInternal.get(col));
              }else{
                LOG.info("GBY not on index key");
              }
              //int sumPos = oldConf.getKeys().size();

              encd.setTabAlias(null);
              exprNodeDesc = encd;
            }
            paraList.set(i, exprNodeDesc);
          }
          oldAggrList.add(aggregationDesc);
        }
      }

      Map<String, ExprNodeDesc> newGbyColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
      Map<String, ExprNodeDesc> oldGbyColExprMap = oldGbyOperator.getColumnExprMap();
      Set<String> internalNameSet = oldGbyColExprMap.keySet();
      for (String internal : internalNameSet) {
        ExprNodeDesc exprNodeDesc  = oldGbyColExprMap.get(internal).clone();
        if(exprNodeDesc instanceof ExprNodeColumnDesc){
          ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
          String col = encd.getColumn();
          if(indexKeyNames.contains(col)){
            encd.setColumn(aliasToInternal.get(col));
          }else{
            LOG.info("GBY not on index key");
          }

        }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
          List<ExprNodeDesc> colExprs = ((ExprNodeGenericFuncDesc)exprNodeDesc).getChildExprs();
          for (ExprNodeDesc colExpr : colExprs) {
            if(colExpr instanceof ExprNodeColumnDesc){
              ExprNodeColumnDesc encd = (ExprNodeColumnDesc)colExpr;
              String col = encd.getColumn();
              if(indexKeyNames.contains(col)){
                encd.setColumn(aliasToInternal.get(col));
              }else{
                LOG.info("GBY not on index key");
              }
            }
          }

        }
        newGbyColExprMap.put(internal, exprNodeDesc);
      }


      ArrayList<ExprNodeDesc> newGbyKeys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> oldGbyKeys = oldConf.getKeys();
      for (int i =0; i< oldGbyKeys.size(); i++) {
        ExprNodeDesc exprNodeDesc = oldGbyKeys.get(i).clone();
        if(exprNodeDesc instanceof ExprNodeColumnDesc){
          ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
          String col = encd.getColumn();
          if(indexKeyNames.contains(col)){
            encd.setColumn(aliasToInternal.get(col));
          }else{
            LOG.info("GBY not on index key");
          }
          exprNodeDesc = encd;
        }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
          ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc)exprNodeDesc;
          List<ExprNodeDesc> colExprs = engfd.getChildExprs();
          for (ExprNodeDesc colExpr : colExprs) {
            if(colExpr instanceof ExprNodeColumnDesc){
              ExprNodeColumnDesc encd = (ExprNodeColumnDesc)colExpr;
              String col = encd.getColumn();
              if(indexKeyNames.contains(col)){
                encd.setColumn(aliasToInternal.get(col));
              }else{
                LOG.info("GBY not on index key");
              }

            }
          }
        }
        newGbyKeys.add(exprNodeDesc);
      }

      RowSchema oldRS = oldGbyOperator.getSchema();

      ArrayList<ColumnInfo> oldSign = oldRS.getSignature();
      ArrayList<ColumnInfo> newSign = new ArrayList<ColumnInfo>();
      for (ColumnInfo columnInfo : oldSign) {
        columnInfo.setAlias(null);
        newSign.add(columnInfo);
      }


      List<Operator<? extends Serializable>> childrenList = oldGbyOperator.getChildOperators();
      Operator<? extends Serializable> childGBY = null;
      while(childrenList != null && childrenList.size() > 0){
        for (Operator<? extends Serializable> operator : childrenList) {
          if(operator != null){
            if(operator instanceof GroupByOperator){
              childGBY = operator;
              childrenList = null;
              break;
            }else{
              childrenList = operator.getChildOperators();
              continue;
            }
          }
        }
      }

      GroupByDesc childConf = (GroupByDesc) childGBY.getConf();
      ArrayList<AggregationDesc> childAggrList = childConf.getAggregators();
      if(childAggrList != null && childAggrList.size() > 0){
        for (AggregationDesc aggregationDesc : childAggrList) {
          aggregationDesc.setGenericUDAFEvaluator(eval);
          aggregationDesc.setGenericUDAFName("sum");
        }
      }


      oldRS.setSignature(newSign);
      oldGbyOperator.setSchema(oldRS);
      oldConf.setKeys(newGbyKeys);
      oldConf.setAggregators(oldAggrList);
      oldGbyOperator.setColumnExprMap(newGbyColExprMap);
      oldGbyOperator.setConf(oldConf);

    }



  }

  public class AppendSubqueryProcCtx implements NodeProcessorCtx {
  }


}

