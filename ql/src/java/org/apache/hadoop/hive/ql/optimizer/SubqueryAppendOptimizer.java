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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class SubqueryAppendOptimizer implements Transform {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());;
  ParseContext pctx = null;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    this.pctx = pctx;
    //toStringTree(pctx);
        // process group-by pattern
    opRules.put(new RuleRegExp("R1", "TS%"),
        getAppendSubqueryProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        new SubqueryOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    toStringTree(pctx);
    return pctx;
  }

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
            //LOG.info("Operator Identifier =" + Integer.parseInt(operator.getIdentifier())+ " parent - " + pList.getName() + "....child - " + operator.getName());
            LOG.info("Processing for Parent = " + pList.getName() + "("
                + ((Operator<? extends Serializable>) pList).getIdentifier() + ")"
                + " And Child = " + operator.getName() + "("
                + ((Operator<? extends Serializable>) operator).getIdentifier() + ")" );
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

  private NodeProcessor getAppendSubqueryProc(ParseContext pctx) {
    return new AppendSubqueryProcessor(pctx);
  }

  /**
   * BucketSubqueryProcessor.
   *
   */
  public class AppendSubqueryProcessor implements NodeProcessor {

    public AppendSubqueryProcessor(ParseContext pContext) {
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());

      //TableScanOperator tsOrigOp = (TableScanOperator)nd;
      ParseContext subPCtx = generateDAGForSubquery();
      //appendSubquery(tsOrigOp, tsSubqOp);
      appendSubquery(pctx, subPCtx);

      return pctx;
    }

    private ParseContext generateDAGForSubquery(){
      HiveConf conf = pctx.getConf();
      Context ctx;
      Operator<Serializable> sinkOp = null;
      ParseContext subPCtx = null;
      try {
        ctx = new Context(conf);
      String command = "Select key, count(key) from tbl group by key";
      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      sinkOp = doSemanticAnalysis(ctx, sem, tree);

      //sem.analyze(tree, ctx);
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


    private void appendSubquery(ParseContext origPCtx, ParseContext subqPCtx){
      List<Operator<? extends Serializable>> finalDAG = new ArrayList<Operator<? extends Serializable>>();
      int id = 1;

      HashMap<String, Operator<? extends Serializable>> origTop = origPCtx.getTopOps();
      Iterator<String> origTabItr = origTop.keySet().iterator();
      String origTab = origTabItr.next();
      Operator<? extends Serializable> origOp = origTop.get(origTab);
      List<Operator<? extends Serializable>> origChildrenList = origOp.getChildOperators();

      //int oId = 0;
      //oId = Integer.parseInt(origOp.getIdentifier());
     // LOG.info("Orig Top Operator " + origOp.getName() + "(" + oId + ")" );

      HashMap<String, Operator<? extends Serializable>> subqTop = subqPCtx.getTopOps();
      Iterator<String> subqTabItr = subqTop.keySet().iterator();
      String subqTab = subqTabItr.next();
      List<Operator<? extends Serializable>> subqList;
      Operator<? extends Serializable> subqOp = subqTop.get(subqTab);
      //LOG.info("Sub-query hierarchy");

      if(subqOp != null && subqOp.getChildOperators() != null){
        origOp.setChildOperators(subqOp.getChildOperators());
      }

      List<Operator<? extends Serializable>> subqFSParentList = null;
      while(subqOp != null && subqOp.getChildOperators() != null && subqOp.getChildOperators().size() > 0){
        subqList = subqOp.getChildOperators();
        if(subqList != null && subqList.size() > 0){
          for (Operator<? extends Serializable> operator : subqList) {
            if(null != operator){
              if(operator instanceof FileSinkOperator) {
                subqFSParentList = operator.getParentOperators();
                subqOp = null;
                break;
              }else{
                operator.setId(Integer.toString(id));
                id++;
                //oId = Integer.parseInt(operator.getIdentifier());
                //LOG.info("Operator " + operator.getName() + "(" + oId + ")" );

                finalDAG.add(operator);
                subqOp = operator;
                continue;
              }
            }
          }
        }
      }


      if(null != origChildrenList) {
        finalDAG.get(finalDAG.size() - 1).setChildOperators(origChildrenList);
      }


     // LOG.info("Original query hierarchy");
      List<Operator<? extends Serializable>> origList = origChildrenList;
        while(origList != null && origList.size() > 0){
          for (Operator<? extends Serializable> operator : origList) {
            if(null != operator){
              if(Integer.parseInt(operator.getIdentifier()) == 1){
                operator.setParentOperators(subqFSParentList);
              }

              operator.setId(Integer.toString(id));
              id++;
              //oId = Integer.parseInt(operator.getIdentifier());
              //LOG.info("Operator " + operator.getName() + "(" + oId + ")" );

              finalDAG.add(operator);
              if(operator.getChildOperators() != null && operator.getChildOperators().size() > 0){
                origList = operator.getChildOperators();
                continue;
              }else{
                origList = null;
                break;
              }
            }
          }
        }
        origOp.setChildOperators(null);
        origOp.setChildOperators(finalDAG);
        origTop.remove(origTab);
        origTop.put(origTab, origOp);
        pctx.setTopOps(origTop);

    }

/*    private void appendSubquery(ParseContext origPCtx, ParseContext subqPCtx){
      List<Operator<? extends Serializable>> finalDAG = new ArrayList<Operator<? extends Serializable>>();
      int finCnt = 0;


      HashMap<String, Operator<? extends Serializable>> origTop = origPCtx.getTopOps();
      Iterator<String> origTabItr = origTop.keySet().iterator();
      String origTab = origTabItr.next();
      List<Operator<? extends Serializable>> origList = null;
      List<Operator<? extends Serializable>> tempParentList = new ArrayList<Operator<? extends Serializable>>();
      Operator<? extends Serializable> origOp = origTop.get(origTab);
      int oId = 0;

      oId = Integer.parseInt(origOp.getIdentifier());
      LOG.info("Operator " + origOp.getName() + "(" + oId + ")" );


      HashMap<String, Operator<? extends Serializable>> subqTop = subqPCtx.getTopOps();
      Iterator<String> subqTabItr = subqTop.keySet().iterator();
      String subqTab = subqTabItr.next();
      List<Operator<? extends Serializable>> subqList;
      Operator<? extends Serializable> subqOp = subqTop.get(subqTab);
      LOG.info("Sub-query hierarchy");



      Operator<? extends Serializable> temp = origOp;
      if(null != subqOp && null != subqOp.getChildOperators()) {
        temp.setChildOperators(subqOp.getChildOperators());
      }
      if(null != origOp && null != origOp.getParentOperators()) {
        temp.setParentOperators(origOp.getParentOperators());
      }
      temp.setId(String.valueOf(finCnt));
      finCnt++;
      finalDAG.add(temp);


      oId = Integer.parseInt(subqOp.getIdentifier());
      while(subqOp != null && subqOp.getChildOperators() != null && subqOp.getChildOperators().size() > 0){
        subqList = subqOp.getChildOperators();
        if(subqList != null && subqList.size() > 0){
          for (Operator<? extends Serializable> operator : subqList) {
            if(null != operator){
              oId = Integer.parseInt(operator.getIdentifier());
              LOG.info("Operator " + operator.getName() + "(" + oId + ")" );
              if(!(operator instanceof FileSinkOperator)) {
                temp = operator;
                if(Integer.parseInt(operator.getIdentifier()) == 1){
                  if(null != operator.getChildOperators().get(0)) {
                    temp.setParentOperators(operator.getChildOperators().get(0).getParentOperators());
                  }
                }else{
                  if(null != operator.getParentOperators()) {
                    temp.setParentOperators(operator.getParentOperators());
                  }
                }
                if(null != operator && null != operator.getChildOperators()) {
                  temp.setChildOperators(operator.getChildOperators());
                }
                temp.setId(String.valueOf(finCnt));
                finCnt++;
                finalDAG.add(temp);
              }else{
                tempParentList = operator.getParentOperators();
              }
              subqOp = operator;
              continue;
            }
          }
        }
      }

        if(null != origOp && null != origOp.getChildOperators()) {
          finalDAG.get(finalDAG.size() - 1).setChildOperators(origOp.getChildOperators());
        }

      LOG.info("Original query hierarchy");
      while(origOp != null && origOp.getChildOperators() != null && origOp.getChildOperators().size() > 0){
        origList = origOp.getChildOperators();
        if(origList != null && origList.size() > 0){
          for (Operator<? extends Serializable> operator : origList) {
            if(null != operator){
              oId = Integer.parseInt(operator.getIdentifier());
              LOG.info("Operator " + operator.getName() + "(" + oId + ")" );
              temp = operator;
              if(Integer.parseInt(operator.getIdentifier()) == 1){
                //temp.setParentOperators(tempParentList);
              }else{
                if(null != operator.getParentOperators()) {
                  temp.setParentOperators(operator.getParentOperators());
                }
              }

              if(null != operator && null != operator.getChildOperators()) {
                temp.setChildOperators(operator.getChildOperators());
              }
              temp.setId(String.valueOf(finCnt));
              finCnt++;
              finalDAG.add(temp);
              origOp = operator;
              continue;
            }
          }
        }
      }


      LOG.info("Final DAG");
      for (Operator<? extends Serializable> operator : finalDAG) {
        if(null != operator){
          oId = Integer.parseInt(operator.getIdentifier());
          LOG.info("Operator " + operator.getName() + "(" + oId + ")" );
        }
      }

      Operator<? extends Serializable> pList = finalDAG.get(0);
      while(pList != null && pList.getChildOperators() != null && pList.getChildOperators().size() > 0){
        List<Operator<? extends Serializable>> cList = pList.getChildOperators();
        for (Operator<? extends Serializable> operator : cList) {
          if(null != operator){
            LOG.info("Operator Identifier =" + Integer.parseInt(operator.getIdentifier())+ " parent - " + pList.getName() + "....child - " + operator.getName());
            pList = operator;
            continue;
            }
        }
      }



    }
*/
  }

  public class SubqueryOptProcCtx implements NodeProcessorCtx {
  }


}
