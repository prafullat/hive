package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.RewriteCanApplyCtx.RewriteVars;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
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

public final class RewriteCanApplyProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteCanApplyProcFactory.class.getName());
  private static RewriteCanApplyCtx canApplyCtx = null;

  private RewriteCanApplyProcFactory(){

  }

  public static class CheckFilterProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator operator = (FilterOperator)nd;
      canApplyCtx = (RewriteCanApplyCtx)ctx;
      FilterDesc conf = (FilterDesc)operator.getConf();
      ExprNodeGenericFuncDesc oldengfd = (ExprNodeGenericFuncDesc) conf.getPredicate();
      if(oldengfd == null){
        canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, true);
        return false;
      }
      List<String> colList = oldengfd.getCols();
      if(colList == null || colList.size() == 0){
        canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, true);
        return false;
      }
      for (String col : colList) {
        canApplyCtx.predColRefs.add(col);
      }

      return null;
    }
  }

 public static CheckFilterProc canApplyOnFilterOperator() {
    return new CheckFilterProc();
  }



 public static class CheckGroupByProc implements NodeProcessor {
   public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
       Object... nodeOutputs) throws SemanticException {
     GroupByOperator operator = (GroupByOperator)nd;
     canApplyCtx = (RewriteCanApplyCtx)ctx;

     if(canApplyCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){
       canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_GROUP_BY, true);
       GroupByDesc conf = (GroupByDesc) operator.getConf();
       ArrayList<AggregationDesc> aggrList = conf.getAggregators();
       if(aggrList != null && aggrList.size() > 0){
           for (AggregationDesc aggregationDesc : aggrList) {
             canApplyCtx.setIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_CNT, canApplyCtx.aggFuncCnt++);
             if(canApplyCtx.getIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_CNT) > 1) {
               return false;
             }
             String aggFunc = aggregationDesc.getGenericUDAFName();
             if(!aggFunc.equals("count")){
               canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_IS_NOT_COUNT, true);
               return false;
             }else{
              ArrayList<ExprNodeDesc> para = aggregationDesc.getParameters();
               if(para == null){
                 canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION, true);
                 return false;
               }else if(para.size() == 0){
                 LOG.info("count(*) case");
                 canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.GBY_NOT_ON_COUNT_KEYS, true);
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
                     canApplyCtx.selColRefNameList.add(((ExprNodeColumnDesc) end).getColumn());
                     canApplyCtx.colRefAggFuncInputList.add(para.get(i).getCols());
                   }
                 }
               }
             }
           }
       }else{
         QBParseInfo qbParseInfo =  canApplyCtx.getParseContext().getQB().getParseInfo();
         Set<String> clauseNameSet = qbParseInfo.getClauseNames();
         if (clauseNameSet.size() != 1) {
           return false;
         }
         Iterator<String> clauseNameIter = clauseNameSet.iterator();
         String clauseName = clauseNameIter.next();
         ASTNode rootSelExpr = qbParseInfo.getSelForClause(clauseName);
         boolean isDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
         if(isDistinct) {
           canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_DISTINCT, true);
         }
       }
       ArrayList<ExprNodeDesc> keyList = conf.getKeys();
       if(keyList == null || keyList.size() == 0){
         canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.GBY_KEYS_FETCH_EXCEPTION, true);
         return false;
       }
       canApplyCtx.setIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.GBY_KEY_CNT, keyList.size());
       for (ExprNodeDesc exprNodeDesc : keyList) {
         if(exprNodeDesc instanceof ExprNodeColumnDesc){
           canApplyCtx.gbKeyNameList.addAll(exprNodeDesc.getCols());
           canApplyCtx.selColRefNameList.add(((ExprNodeColumnDesc) exprNodeDesc).getColumn());
         }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
           ExprNodeGenericFuncDesc endfg = (ExprNodeGenericFuncDesc)exprNodeDesc;
           List<ExprNodeDesc> childExprs = endfg.getChildExprs();
           for (ExprNodeDesc end : childExprs) {
             if(end instanceof ExprNodeColumnDesc){
               canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_KEY_MANIP_FUNC, true);
               canApplyCtx.gbKeyNameList.addAll(exprNodeDesc.getCols());
               canApplyCtx.selColRefNameList.add(((ExprNodeColumnDesc) end).getColumn());
             }
           }
         }
       }

     }

     return null;
   }
 }

 public static CheckGroupByProc canApplyOnGroupByOperator() {
   return new CheckGroupByProc();
 }

 public static class CheckExtractProc implements NodeProcessor {
   public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
       Object... nodeOutputs) throws SemanticException {
     ExtractOperator operator = (ExtractOperator)nd;
     canApplyCtx = (RewriteCanApplyCtx)ctx;

     if(operator.getParentOperators() != null && operator.getParentOperators().size() >0){
       Operator<? extends Serializable> interim = operator.getParentOperators().get(0);
       if(interim instanceof ReduceSinkOperator){
         ReduceSinkDesc conf = (ReduceSinkDesc) interim.getConf();
         ArrayList<ExprNodeDesc> partCols = conf.getPartitionCols();
         int nr = conf.getNumReducers();
         if(nr == -1){
           if(partCols != null && partCols.size() > 0){
             canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_DISTRIBUTE_BY, true);
             return false;
           }else{
             canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_SORT_BY, true);
             return false;
           }
         }else if(nr == 1){
           canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_ORDER_BY, true);
           return false;
         }

       }
     }

     return null;
   }
 }

 public static CheckExtractProc canApplyOnExtractOperator() {
   return new CheckExtractProc();
 }

 public static class CheckSelectProc implements NodeProcessor {
   public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
       Object... nodeOutputs) throws SemanticException {
     SelectOperator operator = (SelectOperator)nd;
     canApplyCtx = (RewriteCanApplyCtx)ctx;

     List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
     Operator<? extends Serializable> child = childrenList.get(0);
     if(child instanceof GroupByOperator){
       return true;
     }else if(child instanceof FileSinkOperator){
       Map<String, String> internalToAlias = new LinkedHashMap<String, String>();
       RowSchema rs = operator.getSchema();
       ArrayList<ColumnInfo> sign = rs.getSignature();
       for (ColumnInfo columnInfo : sign) {
         internalToAlias.put(columnInfo.getInternalName(), columnInfo.getAlias());
       }
       for (int i=0 ; i< canApplyCtx.predColRefs.size(); i++) {
         String predCol = canApplyCtx.predColRefs.get(i);
         if(predCol.startsWith("_c") && internalToAlias.get(predCol) != null){
           canApplyCtx.predColRefs.set(i, internalToAlias.get(predCol));
         }
       }

     }else{
       SelectDesc conf = (SelectDesc)operator.getConf();
       ArrayList<ExprNodeDesc> selColList = conf.getColList();
       if(selColList == null || selColList.size() == 0){
         canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.SEL_CLAUSE_COLS_FETCH_EXCEPTION, true);
         return false;
       }else{
         if(!canApplyCtx.getBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_GROUP_BY)){
           for (ExprNodeDesc exprNodeDesc : selColList) {
             if(exprNodeDesc instanceof ExprNodeColumnDesc){
               if(!((ExprNodeColumnDesc) exprNodeDesc).getColumn().startsWith("_c")){
                 canApplyCtx.selColRefNameList.addAll(exprNodeDesc.getCols());
               }
             }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
               ExprNodeGenericFuncDesc endfg = (ExprNodeGenericFuncDesc)exprNodeDesc;
               List<ExprNodeDesc> childExprs = endfg.getChildExprs();
               for (ExprNodeDesc end : childExprs) {
                 if(end instanceof ExprNodeColumnDesc){
                   if(!((ExprNodeColumnDesc) exprNodeDesc).getColumn().startsWith("_c")){
                     canApplyCtx.selColRefNameList.addAll(exprNodeDesc.getCols());
                   }
                 }
               }
             }
           }

         }

       }
   }

     return null;
   }
 }

 public static CheckSelectProc canApplyOnSelectOperator() {
   return new CheckSelectProc();
 }



}
