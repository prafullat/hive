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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
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
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * RewriteRemoveGroupbyCtx  class stores the context for the {@link RewriteRemoveGroupbyProcFactory} processor factory methods
 */
public class RewriteRemoveGroupbyCtx implements NodeProcessorCtx {

  private RewriteRemoveGroupbyCtx(ParseContext parseContext, Hive hiveDb, String indexTableName){
    //this prevents the class from getting instantiated
    this.parseContext = parseContext;
    this.hiveDb = hiveDb;
    this.indexName = indexTableName;
    this.opc = parseContext.getOpParseCtx();
  }

  public static RewriteRemoveGroupbyCtx getInstance(ParseContext parseContext, Hive hiveDb, String indexTableName){
    return new RewriteRemoveGroupbyCtx(parseContext, hiveDb, indexTableName);
  }

  //We need these two ArrayLists to reset the parent operator list and child operator list in the operator tree
  // once we remove the operators that represent the group-by construct
  private final List<Operator<? extends Serializable>>  newParentList = new ArrayList<Operator<? extends Serializable>>();
  private final List<Operator<? extends Serializable>>  newChildrenList = new ArrayList<Operator<? extends Serializable>>();

  //We need to remove the operators from OpParseContext to remove them from the operator tree
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
  private final Hive hiveDb;
  private final ParseContext parseContext;

  private final String indexName;

  public List<Operator<? extends Serializable>> getNewParentList() {
    return newParentList;
  }

  public List<Operator<? extends Serializable>> getNewChildrenList() {
    return newChildrenList;
  }

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

  /**
   * Given a root node of the parse tree, this function returns the "first" TOK_FUNCTION node
   * that matches the input function name
   *
   * @param root
   * @return
   */
  ASTNode getFuncNode(ASTNode root, String funcName){
    ASTNode func = null;
    ArrayList<Node> cList = root.getChildren();
    while(cList != null && cList.size() > 0){
      for (Node node : cList) {
        if(null != node){
          ASTNode curr = (ASTNode)node;
          if(curr.getType() == HiveParser.TOK_TABLE_OR_COL){
            ArrayList<Node> funcChildren = curr.getChildren();
            for (Node child : funcChildren) {
              ASTNode funcChild = (ASTNode)child;
              if(funcChild.getText().equals(funcName)){
                func = curr;
                cList = null;
                break;
              }
            }
          }else{
            cList = curr.getChildren();
            continue;
          }
        }
      }
    }
    return func;
  }


  /**
   * Given an input operator, this function returns the top TableScanOperator for the operator tree
   * @param inputOp
   * @return
   */
  Operator<? extends Serializable> getTopOperator(Operator<? extends Serializable> inputOp){
    Operator<? extends Serializable>  tsOp = null;
    List<Operator<? extends Serializable>> parentList = inputOp.getParentOperators();
    while(parentList != null && parentList.size() > 0){
      for (Operator<? extends Serializable> op : parentList) {
        if(op != null){
          if(op instanceof TableScanOperator){
            tsOp = (TableScanOperator) op;
            parentList = null;
            break;
          }else{
            parentList = op.getParentOperators();
            continue;
          }
        }
      }
    }

    return tsOp;
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
  public void invokeRemoveGbyProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // replace scan operator containing original table with index table
    opRules.put(new RuleRegExp("R1", "TS%"), RewriteRemoveGroupbyProcFactory.getReplaceTableScanProc());
    //rule that replaces index key selection with size(_offsets) function in original query
    opRules.put(new RuleRegExp("R2", "SEL%"), RewriteRemoveGroupbyProcFactory.getReplaceIdxKeyWithSizeFuncProc());
    // remove group-by pattern from original operator tree
    opRules.put(new RuleRegExp("R3", "GBY%RS%GBY%"), RewriteRemoveGroupbyProcFactory.getRemoveGroupByProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);

  }


  /**
   * Walk the original operator tree using the {@link PreOrderWalker} using the rules.
   * Each of the rules invoke respective methods from the {@link RewriteRemoveGroupbyProcFactory}
   * to replace the original {@link TableScanOperator} with the new index table scan operator.
   *
   * @param topOp
   * @throws SemanticException
   */
  public void invokeReplaceTableScanProc(Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // replace scan operator containing original table with index table
    opRules.put(new RuleRegExp("R1", "TS%"), RewriteRemoveGroupbyProcFactory.getReplaceTableScanProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new PreOrderWalker(disp);

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

