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
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
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
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  /***************************************Can Apply Optimization Flags***************************************/
  int NO_OF_SUBQUERIES = 0;
  int NO_OF_TABLES = 0;
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
  boolean SHOULD_APPEND_SUBQUERY = false;
  boolean REMOVE_GROUP_BY = false;
  boolean QUERY_HAS_KEY_MANIP_FUNC = false;

  /***************************************Index Validation Variables***************************************/
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
  private static final String COMPACT_IDX_BUCKET_COL = "_bucketname";
  private static final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";
  private String topTable = null;
  private final List<String> selColRefNameList = new ArrayList<String>();
  private List<String> predColRefs = new ArrayList<String>();
  private final List<String> gbKeyNameList = new ArrayList<String>();
  private RewriteGBUsingIndexProcCtx rewriteContext = new RewriteGBUsingIndexProcCtx();
  private final List<List<String>> colRefAggFuncInputList = new ArrayList<List<String>>();
  //private final HashMap<String, String> colInternalToExternalMap = new LinkedHashMap<String, String>();
  private String selReplacementCommand = null;
  private String indexName = null;
  private String subqueryCommand = null;


  /***************************************SubqueryAppend Variables***************************************/
  ParseContext subqueryPctx = null;

  //Initialise all data structures required to copy RowResolver and RowSchema from subquery DAG to original DAG operators
  ArrayList<String> newOutputCols = new ArrayList<String>();
  Map<String, ExprNodeDesc> newColExprMap = new HashMap<String, ExprNodeDesc>();
  ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
  ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
  RowResolver newRR = new RowResolver();

  // Get the parentOperatos List for FileSinkOperator. We need this later to set the parentOperators for original DAG operator
  List<Operator<? extends Serializable>> subqFSParentList = null;
  //We need the reference to this SelectOperator so that the original DAG can be appended here
  Operator<? extends Serializable> subqLastSelectOp = null;

  //OperatorToParseContexts for original DAG and subquery DAG
  LinkedHashMap<Operator<? extends Serializable>, OpParseContext> origOpOldOpc = null;
  LinkedHashMap<Operator<? extends Serializable>, OpParseContext> subqOpOldOpc = null;
  /****************************************************************************************************/

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
    rewriteContext.parseContext = parseContext;


    if(isQueryInValid()){
        return parseContext;
    }else{
      if(shouldApplyOptimization()){
        LOG.info("Applying Rewrite to Original Query.");
        reWriteOriginalQuery();
        return parseContext;
      }else{
        return null;
      }
    }

  }

  private void reWriteOriginalQuery() throws SemanticException{
    //TO-DO

    if(REMOVE_GROUP_BY){
      removeGroupByOps();
      selReplacementCommand = "select size(`_offsets`) from dummyTable";
      replaceTSOp();
      replaceSelOP();
    }

    if(SHOULD_APPEND_SUBQUERY){
      subqueryCommand = "select year(l_shipdate) as YEAR, size(`_offsets`) as CNT from default__lineitem_lineitem_lshipdate_idx__";
      generateSubquery();
      processSubquery();
      processOriginalDAG();
      selReplacementCommand = "select L_ORDERKEY, sum(L_ORDERKEY) as TOTAL from lineitem group by L_ORDERKEY";
      replaceSelOP();
    }

    toStringTree(parseContext);
  }

  private void removeGroupByOps() throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    toStringTree(parseContext);
        // process group-by pattern
    opRules.put(new RuleRegExp("R1", "TS%"),
        getRemoveGroupByProc());
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        new GroupByOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    toStringTree(parseContext);
  }

  private void replaceTSOp(){
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "TS%"), new RewriteTSProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new RewriteDefaultProc(), opRules, rewriteContext);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  private void replaceSelOP(){
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "TS%"), new RewriteSelOpProc());
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new RewriteDefaultProc(), opRules, rewriteContext);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
  private boolean isQueryInValid(){
    QBParseInfo qbParseInfo =  parseContext.getQB().getParseInfo();
    return qbParseInfo.isInsertToTable();
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
      NO_OF_SUBQUERIES = 0;
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
        return false;
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
            result = processSelectOperator(operator);
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
        result = checkIndexInformation(indexTables);
    }
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

  private boolean processSelectOperator(Operator<? extends Serializable> operator){
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
        if(QUERY_HAS_GROUP_BY){
/*          RowSchema rs = operator.getSchema();
          ArrayList<ColumnInfo> colInfo = rs.getSignature();
          for (ColumnInfo columnInfo : colInfo) {
            if(columnInfo.getAlias() != null && (!columnInfo.getAlias().contains("_c"))){
              selColRefNameList.add(columnInfo.getAlias());
              colInternalToExternalMap.put(columnInfo.getInternalName(), columnInfo.getAlias());
            }
          }
*/        }else{
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


  private boolean checkIndexInformation(List<Index> indexTables){
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
        rewriteContext.addTable(topTable, index.getIndexTableName());
      }else{
        SHOULD_APPEND_SUBQUERY = true;
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


  public class RewriteGBUsingIndexProcCtx implements NodeProcessorCtx {
    private ParseContext parseContext;
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

  public static class RewriteDefaultProc implements NodeProcessor {

    //No-Op
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      return null;
    }

  }

    public class RewriteSelOpProc implements NodeProcessor {
      private Map<String, ExprNodeDesc> tsSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
      private Map<String, ExprNodeDesc> gbySelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
      private final Map<String, ExprNodeDesc> fsSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();

      private final ArrayList<ExprNodeDesc> gbySelColList = new ArrayList<ExprNodeDesc>();
      private final ArrayList<ExprNodeDesc> fsSelColList = new ArrayList<ExprNodeDesc>();

      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {

        if(SHOULD_APPEND_SUBQUERY){
          processGenericUDAF();
        }

        Operator<? extends Serializable> tsOp = (TableScanOperator)nd;
        Operator<? extends Serializable> selOp = null;
        List<Operator<? extends Serializable>> childOps = tsOp.getChildOperators();

        while(childOps != null && childOps.size() > 0){
          for (Operator<? extends Serializable> operator : childOps) {
            if(operator != null){
              if(operator instanceof SelectOperator){
                selOp = operator;
                if(SHOULD_APPEND_SUBQUERY){
                  processSelOp(selOp);
                  childOps = operator.getChildOperators();;
                  continue;
                }else{
                  processGenericUDF(selOp);
                  childOps = null;
                  break;
                }
              }else if(operator instanceof FileSinkOperator){
                childOps = null;
                break;
              }else{
                childOps = operator.getChildOperators();
                continue;
              }
            }
          }
        }


        return null;
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
        }else if(childOp instanceof FileSinkOperator){
          selOperator.setColumnExprMap(fsSelColExprMap);
          selDesc.setColList(fsSelColList);
        }

        LOG.info("Coming here");
      }

      private void processGenericUDAF() throws SemanticException{
        SubqueryParseContextGenerator newDAGCreator = new SubqueryParseContextGenerator();
        ParseContext newDAGCtx = newDAGCreator.generateDAGForSubquery(selReplacementCommand);
        Map<GroupByOperator, Set<String>> newGbyOpMap = newDAGCtx.getGroupOpToInputTables();
        GroupByOperator newGbyOperator = newGbyOpMap.keySet().iterator().next();


        Map<GroupByOperator, Set<String>> oldGbyOpMap = parseContext.getGroupOpToInputTables();
        GroupByOperator oldGbyOperator = oldGbyOpMap.keySet().iterator().next();
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

        ExprNodeColumnDesc encdExpr = null;

        GroupByDesc newConf = newGbyOperator.getConf();
        ArrayList<AggregationDesc> newAggrList = newConf.getAggregators();
        if(newAggrList != null && newAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : newAggrList) {
            ArrayList<ExprNodeDesc> paraList = aggregationDesc.getParameters();
            for (int i=0; i< paraList.size(); i++) {
              ExprNodeDesc exprNodeDesc = paraList.get(i);
              if(exprNodeDesc instanceof ExprNodeColumnDesc){
                ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
                encd.setColumn("_col1");
                encd.setTabAlias(null);
                exprNodeDesc = encd;
                encdExpr = (ExprNodeColumnDesc) encd.clone();
              }
              paraList.set(i, exprNodeDesc);
            }
            oldAggrList.add(aggregationDesc);
          }
        }

        Map<String, ExprNodeDesc> newColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
        ArrayList<ExprNodeDesc> newKeys = new ArrayList<ExprNodeDesc>();
        encdExpr.setColumn("_col0");
        newColExprMap.put("_col0", encdExpr);
        newKeys.add(encdExpr);

        RowSchema oldRS = oldGbyOperator.getSchema();

        ArrayList<ColumnInfo> oldSign = oldRS.getSignature();
        ArrayList<ColumnInfo> newSign = new ArrayList<ColumnInfo>();
        for (ColumnInfo columnInfo : oldSign) {
          columnInfo.setAlias(null);
          newSign.add(columnInfo);
        }
        oldRS.setSignature(newSign);
        oldGbyOperator.setSchema(oldRS);
        oldConf.setKeys(newKeys);
        oldConf.setAggregators(oldAggrList);
        oldGbyOperator.setColumnExprMap(newColExprMap);
        oldGbyOperator.setConf(oldConf);
        parseContext.setGroupOpToInputTables(oldGbyOpMap);

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
        try {
        ctx = new Context(conf);
        ParseDriver pd = new ParseDriver();
        tree = pd.parse(selReplacementCommand, ctx);
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

/*
        ArrayList<String> outputColumnNames = new ArrayList<String>();
        outputColumnNames.add("_col0");
        selDesc.setOutputColumnNames(outputColumnNames);
*/

      }



    }


  public class RewriteTSProc implements NodeProcessor {

    public void setUpTableDesc(TableScanDesc tsDesc,Table table, String alias)  {

      tsDesc.setGatherStats(false);

      String tblName = table.getTableName();
      tableSpec tblSpec = createTableScanDesc(table, alias);
      String k = tblName + Path.SEPARATOR;
      tsDesc.setStatsAggPrefix(k);
    }

    private tableSpec createTableScanDesc(Table table, String alias) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      RewriteGBUsingIndexProcCtx rewriteContext = (RewriteGBUsingIndexProcCtx)procCtx;

      TableScanOperator scanOperator = (TableScanOperator)nd;
      HashMap<TableScanOperator, Table>  topToTable =
        rewriteContext.parseContext.getTopToTable();

      String baseTableName = topToTable.get(scanOperator).getTableName();
      if( rewriteContext.findBaseTable(baseTableName) == null ) {
        return null;
      }

      //Get the lineage information corresponding to this
      //and modify it ?
      TableScanDesc indexTableScanDesc = new TableScanDesc();
      indexTableScanDesc.setGatherStats(false);

      String tableName = rewriteContext.findBaseTable(baseTableName);

      tableSpec ts = new tableSpec(rewriteContext.hiveDb,
          rewriteContext.parseContext.getConf(),
          tableName
      );
      String k = tableName + Path.SEPARATOR;
      indexTableScanDesc.setStatsAggPrefix(k);
      scanOperator.setConf(indexTableScanDesc);

      topToTable.remove(scanOperator);
      //Scan operator now points to other table
      topToTable.put(scanOperator, ts.tableHandle);
      rewriteContext.parseContext.setTopToTable(topToTable);

      OpParseContext operatorContext =
        rewriteContext.parseContext.getOpParseCtx().get(scanOperator);
      RowResolver rr = new RowResolver();

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
      return null;
    }
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


  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  private void generateSubquery() throws SemanticException{
    origOpOldOpc = parseContext.getOpParseCtx();
    subqueryPctx = (new SubqueryParseContextGenerator()).generateDAGForSubquery(subqueryCommand);

/*    //Creates the operator DAG for subquery
    Dispatcher disp = new DefaultRuleDispatcher(new SubqueryParseContextGenerator(), new LinkedHashMap<Rule, NodeProcessor>(),
        new SubqueryOptProcCtx());
    DefaultGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
*/    subqOpOldOpc = subqueryPctx.getOpParseCtx();
  }

  private void processSubquery() throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    //appends subquery DAG to original DAG
    opRules.put(new RuleRegExp("R1", "FS%"), new SubqueryDAGProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        new SubqueryOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(subqueryPctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

  }

  private void processOriginalDAG() throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    //appends subquery DAG to original DAG
    opRules.put(new RuleRegExp("R1", "TS%"), new OriginalDAGProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        new SubqueryOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

  }



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

/*    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      subqueryPctx = generateDAGForSubquery(subqueryCommand);
      return null;
    }
*/
  }


  public class OriginalDAGProc implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      appendSubquery();
      return null;
    }

    @SuppressWarnings("unchecked")
    private void appendSubquery(){

      //origOp is the TableScanOperator for the original DAG
      HashMap<String, Operator<? extends Serializable>> origTopMap = parseContext.getTopOps();
      Iterator<String> origTabItr = origTopMap.keySet().iterator();
      String origTab = origTabItr.next();
      Operator<? extends Serializable> origOp = origTopMap.get(origTab);
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
      if(subqLastSelectOp != null){
        subqLastSelectOp.setChildOperators(origChildrenList);
      }

      //Loop through the rest of the original operator DAG and set appropriate column informations, RowResolvers and RowSchema
      List<Operator<? extends Serializable>> origList = origChildrenList;
        while(origList != null && origList.size() > 0){
          for (Operator<? extends Serializable> operator : origList) {
            if(null != operator){

              //Copy colList and outputColumns for SelectOperator from sub-query DAG SelectOperator
              if(operator instanceof SelectOperator) {
                //Set columnExprMap + RowSchema + RowResolver - required by all operators
                operator.setColumnExprMap(newColExprMap);
                origOpOldOpc.get(operator).setRowResolver(newRR);
                if(!(operator.getChildOperators().iterator().next() instanceof FileSinkOperator)){
                  operator.getSchema().setSignature(newRS);
                }


                SelectDesc conf = (SelectDesc) operator.getConf();
                conf.setColList(newColList);
                conf.setOutputColumnNames(newOutputCols);
              //Copy output columns of SelectOperator from sub-query DAG to predicates of FilterOperator
              }else if(operator instanceof FilterOperator) {
                //Set columnExprMap + RowSchema + RowResolver - required by all operators
                operator.setColumnExprMap(newColExprMap);
                origOpOldOpc.get(operator).setRowResolver(newRR);

                FilterDesc conf = (FilterDesc)operator.getConf();
                ExprNodeGenericFuncDesc oldengfd = (ExprNodeGenericFuncDesc) conf.getPredicate().clone();
                List<ExprNodeDesc> endExprList = oldengfd.getChildExprs();
                List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>();

                for (ExprNodeDesc exprNodeDesc : endExprList) {
                  if(exprNodeDesc instanceof ExprNodeColumnDesc){
                    ExprNodeColumnDesc encd = (ExprNodeColumnDesc) exprNodeDesc.clone();
                    encd.setColumn(newOutputCols.get(0));
                    newChildren.add(encd);
                  }else{
                    newChildren.add(exprNodeDesc);
                  }
                }
                oldengfd.setChildExprs(newChildren);
                conf.setPredicate(oldengfd);
              //Break loop if FileSinkOperator is visited in the original DAG
              }else if(operator instanceof FileSinkOperator) {
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
        HashMap<TableScanOperator, Table> newTopToTable = (HashMap<TableScanOperator, Table>) parseContext.getTopToTable().clone();
        newTopToTable.remove(origOp);

        HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryPctx.getTopOps();
        Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
        String subqTab = subqTabItr.next();
        Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);

        Table tbl = subqueryPctx.getTopToTable().get(subqOp);
        newTopToTable.put((TableScanOperator) subqOp, tbl);
        parseContext.setTopToTable(newTopToTable);
        parseContext.setTopOps(subqTopMap);
        parseContext.setOpParseCtx(subqOpOldOpc);

    }


  }


  /*
   * Class to append operator DAG of subquery to original operator DAG
   */
  public class SubqueryDAGProc implements NodeProcessor {


    public SubqueryDAGProc() {
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      FileSinkOperator fs = (FileSinkOperator) nd;
      getSelectOperatorSchema(fs);
      return null;
    }

    private void getSelectOperatorSchema(FileSinkOperator fs){
      subqFSParentList = fs.getParentOperators();
      subqLastSelectOp =  subqFSParentList.get(0);
      //Retrieve information only if this is a SelectOperator (This will be true for all operator DAGs
      if(subqLastSelectOp instanceof SelectOperator){
          RowResolver oldRR = subqOpOldOpc.get(subqLastSelectOp).getRowResolver();
          SelectDesc oldConf = (SelectDesc) subqLastSelectOp.getConf();
          Map<String, ExprNodeDesc> oldColumnExprMap = subqLastSelectOp.getColumnExprMap();
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
                //Fetch columnExprMap (required by SelectOperator and FilterOperator in original DAG)
              }else if(end instanceof ExprNodeConstantDesc){

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
      }

      subqOpOldOpc.remove(fs);
      subqueryPctx.setOpParseCtx(subqOpOldOpc);

    }

  }

  public class SubqueryOptProcCtx implements NodeProcessorCtx {
  }


  /********************************GROUP BY Remove Procedure******************************************/
  private NodeProcessor getRemoveGroupByProc() {
    return new RemoveGroupByProcessor();
  }

  public class RemoveGroupByProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      // GBY,RS,GBY... (top to bottom)
      TableScanOperator op = (TableScanOperator) nd;
      removeGroupBy(op);
      return null;
    }

    private void removeGroupBy(TableScanOperator curr){
      List<Operator<? extends Serializable>>  newParentList = new ArrayList<Operator<? extends Serializable>>();
      List<Operator<? extends Serializable>>  newChildrenList = new ArrayList<Operator<? extends Serializable>>();
      List<Operator<? extends Serializable>>  currChildren = new ArrayList<Operator<? extends Serializable>>();
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = parseContext.getOpParseCtx();

      currChildren = curr.getChildOperators();
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

/*                ArrayList<String> outputColumnNames = new ArrayList<String>();
                outputColumnNames.add("_col0");
                selDesc.setOutputColumnNames(outputColumnNames);
*/
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

  }

  public class GroupByOptProcCtx implements NodeProcessorCtx {
  }




}

