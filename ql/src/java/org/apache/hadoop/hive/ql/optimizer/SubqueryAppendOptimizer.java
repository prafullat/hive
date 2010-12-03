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
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class SubqueryAppendOptimizer implements Transform {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());;


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    toStringTree(pctx);
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

  private NodeProcessor getAppendSubqueryProc(ParseContext pctx) {
    return new AppendSubqueryProcessor(pctx);
  }

  /**
   * BucketSubqueryProcessor.
   *
   */
  public class AppendSubqueryProcessor implements NodeProcessor {
    protected ParseContext pGraphContext;

    public AppendSubqueryProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing node - " + nd.getName());
      // GBY,RS,GBY... (top to bottom)
      TableScanOperator tsOp = null;
      appendSubquery(tsOp);
      return null;
    }

    private void appendSubquery(TableScanOperator tsOp){


    }

  }


  public class SubqueryOptProcCtx implements NodeProcessorCtx {
  }


}
