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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

/**
 * Factory of methods used by {@link RewriteGBUsingIndex}
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
        canApplyCtx.whrClauseColsFetchException = true;
      }
      //The predicate should have valid left and right columns
      List<String> colList = oldengfd.getCols();
      if(colList == null || colList.size() == 0){
        canApplyCtx.whrClauseColsFetchException = true;
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
           canApplyCtx.queryHasGroupBy == false ){
         canApplyCtx.queryHasGroupBy = true;
         GroupByDesc conf = (GroupByDesc) operator.getConf();
         ArrayList<AggregationDesc> aggrList = conf.getAggregators();
         if(aggrList != null && aggrList.size() > 0){
             for (AggregationDesc aggregationDesc : aggrList) {
               canApplyCtx.aggFuncCnt++;
               //In the current implementation, we do not support more than 1 agg funcs in group-by
               if(canApplyCtx.aggFuncCnt > 1) {
                 return false;
               }
               String aggFunc = aggregationDesc.getGenericUDAFName();
               if(!aggFunc.equals("count")){
                 canApplyCtx.aggFuncIsNotCount = true;
               }else{
                ArrayList<ExprNodeDesc> para = aggregationDesc.getParameters();
                //for a valid aggregation, it needs to have non-null parameter list
                 if(para == null){
                   canApplyCtx.aggFuncColsFetchException = true;
                 }else if(para.size() == 0){
                   //count(*) case
                   canApplyCtx.countOnAllCols = true;
                 }else{
                   for(int i=0; i< para.size(); i++){
                     ExprNodeDesc expr = para.get(i);
                     if(expr instanceof ExprNodeColumnDesc){
                       //Add the columns to RewriteCanApplyCtx's selectColumnsList list to check later
                       //if index keys contain all select clause columns and vice-a-versa
                       //we get the select column 'actual' names only here if we have a agg func along with group-by
                       //SelectOperator has internal names in its colList data structure
                       canApplyCtx.getSelectColumnsList().add(((ExprNodeColumnDesc) expr).getColumn());
                       //Add the columns to RewriteCanApplyCtx's aggFuncColList list to check later
                       //if columns contained in agg func are index key columns
                       canApplyCtx.getAggFuncColList().add(((ExprNodeColumnDesc) expr).getColumn());
                     }
                   }
                 }
               }
             }
         }

         //we need to have non-null group-by keys for a valid group-by operator
         ArrayList<ExprNodeDesc> keyList = conf.getKeys();
         if(keyList == null || keyList.size() == 0){
           canApplyCtx.gbyKeysFetchException = true;
         }
         //sets the no. of keys in group by to be used later to determine if group-by has non-index cols
         //group-by needs to be preserved in such cases (eg.group-by using a function on index key. This is the subquery append case)
         canApplyCtx.gbyKeyCnt = keyList.size();
         for (ExprNodeDesc expr : keyList) {
           checkExpression(expr);
         }
       }
       return null;
     }

     private void checkExpression(ExprNodeDesc expr){
       if(expr instanceof ExprNodeColumnDesc){
         //Add the group-by keys to RewriteCanApplyCtx's gbKeyNameList list to check later
         //if all keys are from index columns
         canApplyCtx.getGbKeyNameList().addAll(expr.getCols());
       }else if(expr instanceof ExprNodeGenericFuncDesc){
         ExprNodeGenericFuncDesc funcExpr = (ExprNodeGenericFuncDesc)expr;
         List<ExprNodeDesc> childExprs = funcExpr.getChildExprs();
         for (ExprNodeDesc childExpr : childExprs) {
           if(childExpr instanceof ExprNodeColumnDesc){
             //Set QUERY_HAS_GENERICUDF_ON_GROUPBY_KEY to true which is used later to determine
             //whether the rewrite is a 'append subquery' case
             //this is true in case the group-by key is a GenericUDF like year,month etc
             canApplyCtx.queryHasGenericUdfOnGroupbyKey = true;
             canApplyCtx.getGbKeyNameList().addAll(expr.getCols());
             canApplyCtx.getSelectColumnsList().add(((ExprNodeColumnDesc) childExpr).getColumn());
           }else if(childExpr instanceof ExprNodeGenericFuncDesc){
             checkExpression(childExpr);
           }
         }
       }
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
               canApplyCtx.queryHasDistributeBy = true;
             }else{
               //we do not need partition columns in case of sort-by
               canApplyCtx.queryHasSortBy = true;
             }
           }else if(nr == 1){
             //Query has order-by only if number of reducers is 1
             canApplyCtx.queryHasOrderBy = true;
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
         }

         //if FilterOperator predicate has internal column names, we need to retrieve the 'actual' column names to
         //check if index keys contain all filter predicate columns and vice-a-versa
         Iterator<String> predItr = canApplyCtx.getPredicateColumnsList().iterator();
         while(predItr.hasNext()){
           String predCol = predItr.next();
           String newPredCol = "";
           if(internalToAlias.get(predCol) != null){
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
