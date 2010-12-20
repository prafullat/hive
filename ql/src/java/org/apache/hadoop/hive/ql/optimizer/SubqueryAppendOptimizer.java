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
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
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
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;


public class SubqueryAppendOptimizer implements Transform {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());;
  ParseContext pctx = null;
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



  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    this.pctx = pctx;
    generateSubquery();
    processSubquery();
    processOriginalDAG();
    toStringTree(pctx);
    return pctx;
  }

  /* Method to print the operators in the DAG and their child operators */
  private void toStringTree(ParseContext pCtx){
    HashMap<String, Operator<? extends Serializable>> top = pCtx.getTopOps();
    Iterator<String> tabItr = top.keySet().iterator();
    String tab = tabItr.next();
    LOG.info("Printing DAG for table:" + tab );
    Operator<? extends Serializable> pList = top.get(tab);

      while(pList != null && pList.getChildOperators() != null && pList.getChildOperators().size() > 0){
        List<Operator<? extends Serializable>> cList = pList.getChildOperators();
        for (Operator<? extends Serializable> operator : cList) {
          if(null != operator){
            LOG.info("Processing for = " + pList.getName() + "("
                + ((Operator<? extends Serializable>) pList).getIdentifier() + ")" );
            pList = operator;
            continue;
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
    origOpOldOpc = pctx.getOpParseCtx();
    //Creates the operator DAG for subquery
    Dispatcher disp = new DefaultRuleDispatcher(new SubqueryParseContextGenerator(), new LinkedHashMap<Rule, NodeProcessor>(),
        new SubqueryOptProcCtx());
    DefaultGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    subqOpOldOpc = subqueryPctx.getOpParseCtx();
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
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

  }



/*
 *Class to instantialte SemanticAnalyzer and create the operator DAG for subquery
 *Method generateDAGForSubquery() returns the new ParseContext object for the subquery DAG
 */
  public class SubqueryParseContextGenerator implements NodeProcessor {

    public SubqueryParseContextGenerator() {
    }

    private ParseContext generateDAGForSubquery(){
      HiveConf conf = pctx.getConf();
      Context ctx;
      ParseContext subPCtx = null;
      try {
        ctx = new Context(conf);
      String command = "Select key from tbl_idx group by key";
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

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      subqueryPctx = generateDAGForSubquery();
      return null;
    }

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
      HashMap<String, Operator<? extends Serializable>> origTopMap = pctx.getTopOps();
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
              //Set columnExprMap + RowSchema + RowResolver - required by all operators
              operator.setColumnExprMap(newColExprMap);
              operator.getSchema().setSignature(newRS);
              origOpOldOpc.get(operator).setRowResolver(newRR);

              //We need to add operator context in the sub-query OpToParseContext Map (subqOpOldOpc) for those operators
              // in original DAG that are appended to sub-query DAG
              RowResolver rr = origOpOldOpc.get(operator).getRowResolver();
              OpParseContext ctx = new OpParseContext(rr);
              subqOpOldOpc.put(operator, ctx);

              //Copy colList and outputColumns for SelectOperator from sub-query DAG SelectOperator
              if(operator instanceof SelectOperator) {
                SelectDesc conf = (SelectDesc) operator.getConf();
                conf.setColList(newColList);
                conf.setOutputColumnNames(newOutputCols);
              //Copy output columns of SelectOperator from sub-query DAG to predicates of FilterOperator
              }else if(operator instanceof FilterOperator) {
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
         * pctx.setOpParseCtx(subqOpOldOpc) sets the subqOpOldOpc OpToParseContext map to the original context
         * pctx.setTopOps(subqTopMap) sets the topOps map to contain the sub-query topOps map
         * pctx.setTopToTable(newTopToTable) sets the original topToTable to contain sub-query top TableScanOperator
         */
        HashMap<TableScanOperator, Table> newTopToTable = (HashMap<TableScanOperator, Table>) pctx.getTopToTable().clone();
        newTopToTable.remove(origOp);

        HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryPctx.getTopOps();
        Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
        String subqTab = subqTabItr.next();
        Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);

        Table tbl = subqueryPctx.getTopToTable().get(subqOp);
        newTopToTable.put((TableScanOperator) subqOp, tbl);
        pctx.setTopToTable(newTopToTable);
        pctx.setTopOps(subqTopMap);
        pctx.setOpParseCtx(subqOpOldOpc);

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
              ExprNodeColumnDesc oldDesc = (ExprNodeColumnDesc) oldColumnExprMap.get(internalName);
              ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) oldDesc.clone();
              newDesc.setColumn(internalName);
              //Fetch columnExprMap (required by SelectOperator and FilterOperator in original DAG)
              newColExprMap.put(internalName, newDesc);
            }

            ExprNodeDesc exprNodeDesc = oldColList.get(i);
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) exprNodeDesc.clone();
            newDesc.setColumn(internalName);
            //Fetch colList (required by SelectOperators in original DAG)
            newColList.add(newDesc);

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


}
