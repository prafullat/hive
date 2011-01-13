package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

public class RewriteRemoveGroupbyCtx implements NodeProcessorCtx {

  protected final  Log LOG = LogFactory.getLog(RewriteRemoveGroupbyCtx.class.getName());

  private List<Operator<? extends Serializable>>  newParentList = new ArrayList<Operator<? extends Serializable>>();
  private List<Operator<? extends Serializable>>  newChildrenList = new ArrayList<Operator<? extends Serializable>>();
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opc = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
  private Hive hiveDb;
  private  ParseContext parseContext = null;
  private RewriteGBUsingIndexCtx rewriteContext = null;
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


  public RewriteGBUsingIndexCtx getRewriteContext() {
    return rewriteContext;
  }

  public void setRewriteContext(RewriteGBUsingIndexCtx rewriteContext) {
    this.rewriteContext = rewriteContext;
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



  ASTNode getFuncNode(ASTNode root){
    ASTNode func = null;
    ArrayList<Node> cList = root.getChildren();
    while(cList != null && cList.size() > 0){
      for (Node node : cList) {
        if(null != node){
          ASTNode curr = (ASTNode)node;
          if(curr.getType() == HiveParser.TOK_FUNCTION){
            func = curr;
            cList = null;
            break;
          }else{
            cList = curr.getChildren();
            continue;
          }
        }
      }
    }
    return func;
  }


  void replaceIdxKeyWithSizeFunc(Operator<? extends Serializable> selOperator) throws SemanticException{

    HiveConf conf = parseContext.getConf();
    Context ctx = null;
    ASTNode tree = null;
    BaseSemanticAnalyzer sem = null;
    String newSelCommand = "select size(`_offsets`) from " + indexName;
    try {
    ctx = new Context(conf);
    ParseDriver pd = new ParseDriver();
    tree = pd.parse(newSelCommand, ctx);
    tree = ParseUtils.findRootNonNullToken(tree);
    sem = SemanticAnalyzerFactory.get(conf, tree);

    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }


    ASTNode funcNode = getFuncNode(tree);
    LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opCtxMap =
      parseContext.getOpParseCtx();
    List<Operator<? extends Serializable>> parentList = selOperator.getParentOperators();
    Operator<? extends Serializable> tsOp = null;
    while(parentList != null && parentList.size() > 0){
      for (Operator<? extends Serializable> operator : parentList) {
        if(operator != null){
          if(operator instanceof TableScanOperator){
            tsOp = operator;
            parentList = null;
            break;
          }else{
            parentList = operator.getParentOperators();
            continue;
          }
        }
      }
    }


    OpParseContext tsCtx = opCtxMap.get(tsOp);
    ExprNodeDesc exprNode = ((SemanticAnalyzer) sem).genExprNodeDesc(funcNode, tsCtx.getRowResolver());
    String aggFuncCol = "";

    if(exprNode instanceof ExprNodeGenericFuncDesc){
      List<ExprNodeDesc> exprList = ((ExprNodeGenericFuncDesc) exprNode).getChildExprs();
      for (ExprNodeDesc exprNodeDesc : exprList) {
        if(exprNodeDesc instanceof ExprNodeColumnDesc){
          aggFuncCol = ((ExprNodeColumnDesc) exprNodeDesc).getColumn();
        }
      }
    }

    SelectDesc selDesc = (SelectDesc) selOperator.getConf();


    RowSchema rs = selOperator.getSchema();
    ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
    ArrayList<ColumnInfo> sign = rs.getSignature();
    for (ColumnInfo columnInfo : sign) {
      String alias = columnInfo.getAlias();
      if(alias != null && alias.contains("_c")){
        columnInfo.setAlias(aggFuncCol);
      }
      newRS.add(columnInfo);
    }
    selOperator.getSchema().setSignature(newRS);

    ArrayList<ExprNodeDesc> colList = selDesc.getColList();
    int i = 0;
    for (; i< colList.size(); i++) {
      ExprNodeDesc exprNodeDesc = colList.get(i);
      if(exprNodeDesc instanceof ExprNodeColumnDesc){
        if(((ExprNodeColumnDesc) exprNodeDesc).getColumn().contains("_c")){
          colList.set(i, exprNode);
          break;
        }
      }
    }

    selDesc.setColList(colList);

    Map<String, ExprNodeDesc> origColExprMap = selOperator.getColumnExprMap();
    Map<String, ExprNodeDesc> newColExprMap = new LinkedHashMap<String, ExprNodeDesc>();

    Set<String> internalNamesList = origColExprMap.keySet();
    for (String internal : internalNamesList) {
      ExprNodeDesc end = origColExprMap.get(internal).clone();
      if(end instanceof ExprNodeColumnDesc){
        if(((ExprNodeColumnDesc) end).getColumn().contains("_c")){
          newColExprMap.put(internal, exprNode);
        }else{
          newColExprMap.put(internal, end);
        }
      }else{
        newColExprMap.put(internal, end);
      }
    }
    selOperator.setColumnExprMap(newColExprMap);

  }

  SelectOperator getSelectOperator(TableScanOperator tsOp){
    SelectOperator selOp = null;
    List<Operator<? extends Serializable>> childOps = tsOp.getChildOperators();

    while(childOps != null && childOps.size() > 0){
      for (Operator<? extends Serializable> operator : childOps) {
        if(operator != null){
          if(operator instanceof SelectOperator){
            selOp = (SelectOperator) operator;
            childOps = null;
            break;
          }else{
            childOps = operator.getChildOperators();
            continue;
          }
        }
      }
    }
    return selOp;
  }




}
