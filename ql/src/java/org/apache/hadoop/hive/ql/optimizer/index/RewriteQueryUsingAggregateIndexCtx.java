package org.apache.hadoop.hive.ql.optimizer.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class RewriteQueryUsingAggregateIndexCtx  implements NodeProcessorCtx {
  private RewriteQueryUsingAggregateIndexCtx(ParseContext parseContext, Hive hiveDb, String indexTableName){

    //this prevents the class from getting instantiated
    this.parseContext = parseContext;
    this.hiveDb = hiveDb;
    this.indexName = indexTableName;
    this.opc = parseContext.getOpParseCtx();
  }

  public static RewriteQueryUsingAggregateIndexCtx getInstance(ParseContext parseContext, Hive hiveDb, String indexTableName){
    return new RewriteQueryUsingAggregateIndexCtx(parseContext, hiveDb, indexTableName);
  }

  //We need to remove the operators from OpParseContext to remove them from the operator tree
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
  private final Hive hiveDb;
  private final ParseContext parseContext;
  //We need the GenericUDAFEvaluator for GenericUDAF function "sum" when we append subquery to original operator tree
  private GenericUDAFEvaluator eval = null;
  private final String indexName;
  private ExprNodeColumnDesc aggrExprNode = null;

  public LinkedHashMap<Operator<? extends Serializable>, OpParseContext> getOpc() {
    return opc;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }


  public Hive getHiveDb() {
    return hiveDb;
  }

 public String getIndexName() {
    return indexName;
  }

 public GenericUDAFEvaluator getEval() {
   return eval;
 }

 public void setEval(GenericUDAFEvaluator eval) {
   this.eval = eval;
 }

 public void setAggrExprNode(ExprNodeColumnDesc aggrExprNode) {
   this.aggrExprNode = aggrExprNode;
 }

 public ExprNodeColumnDesc getAggrExprNode() {
   return aggrExprNode;
 }



 /**
  * Walk the original operator tree using the {@link PreOrderWalker} using the rules.
  * Each of the rules invoke respective methods from the {@link RewriteRemoveGroupbyProcFactory}
  * to remove the group-by constructs from the original query and replace the original
  * {@link TableScanOperator} with the new index table scan operator.
  *
  * @param topOp
  * @throws SemanticException
  */
 public void invokeRewriteQueryProc(Operator<? extends Serializable> topOp) throws SemanticException{
   Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

   // replace scan operator containing original table with index table
   opRules.put(new RuleRegExp("R1", "TS%"), RewriteQueryUsingAggregateIndex.getReplaceTableScanProc());
   //rule that replaces index key selection with size(_offsets) function in original query
   opRules.put(new RuleRegExp("R2", "SEL%"), RewriteQueryUsingAggregateIndex.getNewQuerySelectSchemaProc());
   //Manipulates the ExprNodeDesc from FilterOperator predicate list as per colList data structure from RewriteIndexSubqueryCtx
   //opRules.put(new RuleRegExp("R3", "FIL%"), RewriteQueryUsingAggregateIndex.getNewQueryFilterSchemaProc());
   //Manipulates the ExprNodeDesc from GroupByOperator aggregation list, parameters list \
   //as per colList data structure from RewriteIndexSubqueryCtx
   opRules.put(new RuleRegExp("R4", "GBY%"), RewriteQueryUsingAggregateIndex.getNewQueryGroupbySchemaProc());

   // The dispatcher fires the processor corresponding to the closest matching
   // rule and passes the context along
   Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
   //GraphWalker ogw = new PreOrderWalker(disp);
   GraphWalker ogw = new DefaultGraphWalker(disp);

   // Create a list of topop nodes
   ArrayList<Node> topNodes = new ArrayList<Node>();
   topNodes.add(topOp);
   ogw.startWalking(topNodes, null);
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

}
