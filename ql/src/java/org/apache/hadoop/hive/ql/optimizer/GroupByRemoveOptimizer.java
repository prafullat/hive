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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class GroupByRemoveOptimizer implements Transform {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    toStringTree(pctx);
        // process group-by pattern
    opRules.put(new RuleRegExp("R1", "GBY%RS%"),
        getRemoveGroupByProc(pctx));
    opRules.put(new RuleRegExp("R2", "FS%"),
        getRemoveFsProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        new GroupByOptProcCtx());
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    toStringTree(pctx);
    return pctx;
  }

  private NodeProcessor getRemoveFsProc(ParseContext pctx) {
    return new FSProc();
  }

  public class FSProc implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Properties pt =    ((FileSinkOperator)nd).getConf().getTableInfo().getProperties();
      pt.setProperty("columns", "key");
      ((FileSinkOperator)nd).getConf().getTableInfo().setProperties(pt);
      return null;
    }

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
            LOG.info("Operator Identifier =" + Integer.parseInt(operator.getIdentifier())+ " parent - " + pList.getName() + "....child - " + operator.getName());
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

  private NodeProcessor getRemoveGroupByProc(ParseContext pctx) {
    return new RemoveGroupByProcessor(pctx);
  }

  /**
   * BucketGroupByProcessor.
   *
   */
  public class RemoveGroupByProcessor implements NodeProcessor {
    protected ParseContext pGraphContext;

    public RemoveGroupByProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      // GBY,RS,GBY... (top to bottom)
      GroupByOperator op = (GroupByOperator) stack.get(stack.size() - 2);
      removeGroupBy(op);
      return null;
    }

    private void removeGroupBy(GroupByOperator curr){
      List<Operator<? extends Serializable>> pList = curr.getParentOperators();
      List<Operator<? extends Serializable>> cList = curr.getChildOperators();
      List<Operator<? extends Serializable>> newCList = new ArrayList<Operator<? extends Serializable>>();
      //List<Operator<? extends Serializable>> finalCList = new ArrayList<Operator<? extends Serializable>>();

      LOG.info("Logging Removal of group by");


      if(pList != null && pList.size() > 0){
        for (Operator<? extends Serializable> operator : pList) {
          if(null != operator){
            int pId = Integer.parseInt(operator.getIdentifier());
            //LOG.info("Parent Operator Identifier =" + pId + " Name - " + operator.getName() );
            newCList = removeNode(operator.getName(), cList);
            //finalCList = operator.getChildOperators();
            pId = Integer.parseInt(operator.getIdentifier());
            if(newCList != null && newCList.size() > 0){
              for (Operator<? extends Serializable> cOp : newCList) {
                if(null != operator){
                  int cId = pId + 1;
                  cOp.setId(String.valueOf(cId));
                  //LOG.info("Child Operator Identifier =" + cId );
                  cOp.setParentOperators(pList);
                  //cOp.setChildOperators(null);
                }
              }

            }
            operator.setChildOperators(newCList);
            //LOG.info("Child operator returned is - " + operator.getChildOperators().get(0).getName());
            }
        }
      }


    }

    private List<Operator<? extends Serializable>> removeNode(String parent, List<Operator<? extends Serializable>> cList){
     // LOG.info("Parent passed is - " + parent);
      List<Operator<? extends Serializable>> newCList = new ArrayList<Operator<? extends Serializable>>();

      while(cList != null && cList.size() > 0){
        for (Operator<? extends Serializable> operator : cList) {
          if(null != operator){
            if(operator.getChildOperators() != null && operator.getChildOperators().size() > 0){
              if(operator.getName().equals(parent)){
                cList = null;
                newCList = operator.getChildOperators();
                break;
              }else{
                cList = operator.getChildOperators();
                //LOG.info("Child Operator Identifier =" + Integer.parseInt(operator.getIdentifier())+ " Name - " + operator.getName() );
              }
            }
           }
        }
      }
      return newCList;

    }


  }


  public class GroupByOptProcCtx implements NodeProcessorCtx {
  }


}
