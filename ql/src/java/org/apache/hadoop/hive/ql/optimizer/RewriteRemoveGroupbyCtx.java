package org.apache.hadoop.hive.ql.optimizer;

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

  private RewriteRemoveGroupbyCtx(){
    //this prevents the class from getting instantiated
  }

  public static RewriteRemoveGroupbyCtx getInstance(){
    return new RewriteRemoveGroupbyCtx();
  }

  //We need these two ArrayLists to reset the parent operator list and child operator list in the operator tree
  // once we remove the operators that represent the group-by construct
  private List<Operator<? extends Serializable>>  newParentList = new ArrayList<Operator<? extends Serializable>>();
  private List<Operator<? extends Serializable>>  newChildrenList = new ArrayList<Operator<? extends Serializable>>();

  //We need to remove the operators from OpParseContext to remove them from the operator tree
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
  private Hive hiveDb;
  private ParseContext parseContext;

  //We need the RewriteCanApplyCtx instance to retrieve the mapping from original table to index table in the
  // getReplaceTableScanProc() method of the RewriteRemoveGroupbyProcFactory
  private RewriteCanApplyCtx canApplyCtx;
  private String indexName = "";

  public List<Operator<? extends Serializable>> getNewParentList() {
    return newParentList;
  }

  public void setNewParentList(List<Operator<? extends Serializable>> newParentList) {
    this.newParentList = newParentList;
  }

  public List<Operator<? extends Serializable>> getNewChildrenList() {
    return newChildrenList;
  }

  public void setNewChildrenList(List<Operator<? extends Serializable>> newChildrenList) {
    this.newChildrenList = newChildrenList;
  }

  public LinkedHashMap<Operator<? extends Serializable>, OpParseContext> getOpc() {
    return opc;
  }

  public void setOpc(LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc) {
    this.opc = opc;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }


  public RewriteCanApplyCtx getCanApplyCtx() {
    return canApplyCtx;
  }

  public void setCanApplyCtx(RewriteCanApplyCtx canApplyCtx) {
    this.canApplyCtx = canApplyCtx;
  }

  public Hive getHiveDb() {
    return hiveDb;
  }

  public void setHiveDb(Hive hiveDb) {
    this.hiveDb = hiveDb;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
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
          if(curr.getType() == HiveParser.TOK_FUNCTION){
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

