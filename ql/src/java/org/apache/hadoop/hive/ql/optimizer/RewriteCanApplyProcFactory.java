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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

/**
 * Factory of methods used by {@link RewriteGBUsingIndex} (see checkEachDAGOperator(..) method)
 * to determine if the rewrite optimization can be applied to the input query
 *
 */
public final class RewriteCanApplyProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteCanApplyProcFactory.class.getName());
  private static RewriteCanApplyCtx canApplyCtx = null;

  private RewriteCanApplyProcFactory(){
    //this prevents the class from getting instantiated
  }


  /**
   * Check for conditions in FilterOperator that do not meet rewrite criteria.
   * Set the appropriate variables in {@link RewriteVars} enum.
   */
  private static class CheckFilterProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator operator = (FilterOperator)nd;
      canApplyCtx = (RewriteCanApplyCtx)ctx;
      FilterDesc conf = (FilterDesc)operator.getConf();
      //The filter operator should have a predicate of ExprNodeGenericFuncDesc type.
      //This represents the comparison operator
      ExprNodeGenericFuncDesc oldengfd = (ExprNodeGenericFuncDesc) conf.getPredicate();
      if(oldengfd == null){
        canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, true);
        //return false;
      }
      //The predicate should have valid left and right columns
      List<String> colList = oldengfd.getCols();
      if(colList == null || colList.size() == 0){
        canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, true);
        //return false;
      }
      //Add the predicate columns to RewriteCanApplyCtx's predColRefs list to check later
      //if index keys contain all filter predicate columns and vice-a-versa
      for (String col : colList) {
        canApplyCtx.getPredicateColumnsList().add(col);
      }

      return null;
    }
  }

 public static CheckFilterProc canApplyOnFilterOperator() {
    return new CheckFilterProc();
  }



   /**
   * Check for conditions in GroupByOperator that do not meet rewrite criteria.
   * Set the appropriate variables in {@link RewriteVars} enum.
   *
   */
  private static class CheckGroupByProc implements NodeProcessor {
     public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
         Object... nodeOutputs) throws SemanticException {
       GroupByOperator operator = (GroupByOperator)nd;
       canApplyCtx = (RewriteCanApplyCtx)ctx;
       //for each group-by clause in query, only one GroupByOperator of the GBY-RS-GBY sequence is stored in  getGroupOpToInputTables
       //we need to process only this operator
       //Also, we do not rewrite for cases when same query branch has multiple group-by constructs
       if(canApplyCtx.getParseContext().getGroupOpToInputTables().containsKey(operator) &&
           canApplyCtx.getBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_GROUP_BY) == false ){

         canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_GROUP_BY, true);

         GroupByDesc conf = (GroupByDesc) operator.getConf();
         ArrayList<AggregationDesc> aggrList = conf.getAggregators();
         if(aggrList != null && aggrList.size() > 0){
             for (AggregationDesc aggregationDesc : aggrList) {
               int aggCnt = canApplyCtx.getAggFuncCnt();
               canApplyCtx.setIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_CNT, aggCnt);
               canApplyCtx.setAggFuncCnt(aggCnt + 1);

               //In the current implementation, we do not support more than 1 agg funcs in group-by
               if(canApplyCtx.getIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_CNT) > 1) {
                 return false;
               }
               String aggFunc = aggregationDesc.getGenericUDAFName();
               if(!aggFunc.equals("count")){
                 canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_IS_NOT_COUNT, true);
                 //return false;
               }else{
                ArrayList<ExprNodeDesc> para = aggregationDesc.getParameters();
                //for a valid aggregation, it needs to have non-null parameter list
                 if(para == null){
                   canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION, true);
                   //return false;
                 }else if(para.size() == 0){
                   //count(*) case
                   canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.COUNT_ON_ALL_COLS, true);
                   //return false;
                 }else{
                   for(int i=0; i< para.size(); i++){
                     ExprNodeDesc end = para.get(i);
                     if(end instanceof ExprNodeColumnDesc){
                       //Add the columns to RewriteCanApplyCtx's selectColumnsList list to check later
                       //if index keys contain all select clause columns and vice-a-versa
                       //we get the select column 'actual' names only here if we have a agg func along with groub-by
                       //SelectOperator has internal names in its colList data structure
                       canApplyCtx.getSelectColumnsList().add(((ExprNodeColumnDesc) end).getColumn());

                       //Add the columns to RewriteCanApplyCtx's aggFuncColList list to check later
                       //if columns contained in agg func are index key columns
                       canApplyCtx.getAggFuncColList().add(((ExprNodeColumnDesc) end).getColumn());
                     }
                   }
                 }
               }
             }
         }else{
           //if group-by does not have aggregation list, then it "might" be a DISTINCT case
           //this code uses query block to determine if the ASTNode tree contains the distinct TOK_SELECTDI token
           QBParseInfo qbParseInfo =  canApplyCtx.getParseContext().getQB().getParseInfo();
           Set<String> clauseNameSet = qbParseInfo.getClauseNames();
           if (clauseNameSet.size() == 1) {
             Iterator<String> clauseNameIter = clauseNameSet.iterator();
             String clauseName = clauseNameIter.next();
             ASTNode rootSelExpr = qbParseInfo.getSelForClause(clauseName);
             boolean isDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
             if(isDistinct) {
               canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_DISTINCT, true);
             }
           }
         }


         //we need to have non-null groub-by keys for a valid groub-by operator
         ArrayList<ExprNodeDesc> keyList = conf.getKeys();
         if(keyList == null || keyList.size() == 0){
           canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.GBY_KEYS_FETCH_EXCEPTION, true);
           //return false;
         }

         //sets the no. of keys in groub by to be used later to determine is group-by has non-index cols
         //group-by needs to be preserved in such cases (eg.group-by using a function on index key. This is the subquery append case)
         canApplyCtx.setIntVar(canApplyCtx.getParseContext().getConf(), RewriteVars.GBY_KEY_CNT, keyList.size());
         for (ExprNodeDesc exprNodeDesc : keyList) {
           if(exprNodeDesc instanceof ExprNodeColumnDesc){
             //Add the group-by keys to RewriteCanApplyCtx's gbKeyNameList list to check later
             //if all keys are from index columns
             canApplyCtx.getGbKeyNameList().addAll(exprNodeDesc.getCols());
           }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
             ExprNodeGenericFuncDesc endfg = (ExprNodeGenericFuncDesc)exprNodeDesc;
             List<ExprNodeDesc> childExprs = endfg.getChildExprs();
             for (ExprNodeDesc end : childExprs) {
               if(end instanceof ExprNodeColumnDesc){
                 //Set QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY to true which is used later to determine
                 //whether the rewrite is a 'append subquery' case
                 //this is true in case the group-by key is a GenericUDF like year,month etc
                 canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY, true);
                 canApplyCtx.getGbKeyNameList().addAll(exprNodeDesc.getCols());
                 canApplyCtx.getSelectColumnsList().add(((ExprNodeColumnDesc) end).getColumn());
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


 /**
   * Check for conditions in ExtractOperator that do not meet rewrite criteria.
   * Set the appropriate variables in {@link RewriteVars} enum.
   *
   */
  private static class CheckExtractProc implements NodeProcessor {
     public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
         Object... nodeOutputs) throws SemanticException {
       ExtractOperator operator = (ExtractOperator)nd;
       canApplyCtx = (RewriteCanApplyCtx)ctx;
       //We get the information whether query has SORT BY, ORDER BY, DISTRIBUTE BY from
       //the parent ReduceSinkOperator of the current ExtractOperator
       if(operator.getParentOperators() != null && operator.getParentOperators().size() >0){
         Operator<? extends Serializable> interim = operator.getParentOperators().get(0);
         if(interim instanceof ReduceSinkOperator){
           ReduceSinkDesc conf = (ReduceSinkDesc) interim.getConf();
           ArrayList<ExprNodeDesc> partCols = conf.getPartitionCols();
           int nr = conf.getNumReducers();
           if(nr == -1){
             if(partCols != null && partCols.size() > 0){
               //query has distribute-by is there are non-zero partition columns
               canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_DISTRIBUTE_BY, true);
               //return false;
             }else{
               //we do not need partition columns in case of sort-by
               canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_SORT_BY, true);
               //return false;
             }
           }else if(nr == 1){
             //Query has order-by only if number of reducers is 1
             canApplyCtx.setBoolVar(canApplyCtx.getParseContext().getConf(), RewriteVars.QUERY_HAS_ORDER_BY, true);
             //return false;
           }

         }
       }

       return null;
     }
   }

   public static CheckExtractProc canApplyOnExtractOperator() {
     return new CheckExtractProc();
   }

   /**
   * Check for conditions in SelectOperator that do not meet rewrite criteria.
   * Set the appropriate variables in {@link RewriteVars} enum.
   *
   */
  private static class CheckSelectProc implements NodeProcessor {
     public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
         Object... nodeOutputs) throws SemanticException {
       SelectOperator operator = (SelectOperator)nd;
       canApplyCtx = (RewriteCanApplyCtx)ctx;

       List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
       Operator<? extends Serializable> child = childrenList.get(0);
       if(child instanceof FileSinkOperator){
         Map<String, String> internalToAlias = new LinkedHashMap<String, String>();
         RowSchema rs = operator.getSchema();
         //to get the internal to alias mapping
         ArrayList<ColumnInfo> sign = rs.getSignature();
         for (ColumnInfo columnInfo : sign) {
           internalToAlias.put(columnInfo.getInternalName(), columnInfo.getAlias());
           //Add the columns to RewriteCanApplyCtx's selectColumnsList list to check later
           //if index keys contain all select clause columns and vice-a-versa
           if(!columnInfo.getAlias().startsWith("_c")){
             canApplyCtx.getSelectColumnsList().add(columnInfo.getAlias());
           }
         }

         //if FilterOperator predicate has internal column names, we need to retrieve the 'actual' column names to
         //check if index keys contain all filter predicate columns and vice-a-versa
         Iterator<String> predItr = canApplyCtx.getPredicateColumnsList().iterator();
         while(predItr.hasNext()){
           String predCol = predItr.next();
           String newPredCol = "";
           if(predCol.startsWith("_c") && internalToAlias.get(predCol) != null){
             newPredCol = internalToAlias.get(predCol);
             canApplyCtx.getPredicateColumnsList().remove(predCol);
             canApplyCtx.getPredicateColumnsList().add(newPredCol);
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
