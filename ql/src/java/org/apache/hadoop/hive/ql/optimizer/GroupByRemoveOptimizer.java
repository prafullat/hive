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
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;


public class GroupByRemoveOptimizer implements Transform {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());;
  ParseContext parseContext = null;

  @Override
  public ParseContext transform(ParseContext parseContext) throws SemanticException {
    this.parseContext = parseContext;
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
    return parseContext;
  }


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
