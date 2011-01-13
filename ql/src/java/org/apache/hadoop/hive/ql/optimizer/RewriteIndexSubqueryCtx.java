package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class RewriteIndexSubqueryCtx implements NodeProcessorCtx {
  protected final Log LOG = LogFactory.getLog(RewriteIndexSubqueryCtx.class.getName());

  private Map<String, ExprNodeDesc> tsSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
  private Map<String, ExprNodeDesc> gbySelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
  private ArrayList<ExprNodeDesc> gbySelColList = new ArrayList<ExprNodeDesc>();


  // Initialise all data structures required to copy RowResolver and RowSchema from subquery DAG to
  // original DAG operators
  private ArrayList<String> newOutputCols = new ArrayList<String>();
  private Map<String, ExprNodeDesc> newColExprMap = new HashMap<String, ExprNodeDesc>();
  private ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
  private ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
  private RowResolver newRR = new RowResolver();
  private Map<String, String> aliasToInternal = new LinkedHashMap<String, String>();

  // Get the parentOperators List for FileSinkOperator. We need this later to set the
  // parentOperators for original DAG operator
  private List<Operator<? extends Serializable>> subqFSParentList = null;
  // We need the reference to this SelectOperator so that the original DAG can be appended here
  private Operator<? extends Serializable> subqSelectOp = null;
  private Operator<? extends Serializable> newTSOp = null;
  private ParseContext parseContext = null;
  private ParseContext subqueryPctx = null;
  private ParseContext newDAGCtx = null;
  private final ParseContextGenerator pcg = new ParseContextGenerator();

  private Set<String> indexKeyNames = new LinkedHashSet<String>();
  private String indexName = "";
  private String currentTableName = null;
  private GenericUDAFEvaluator eval = null;


  public Set<String> getIndexKeyNames() {
    return indexKeyNames;
  }

  public void setIndexKeyNames(Set<String> indexKeyNames) {
    this.indexKeyNames = indexKeyNames;
  }

  public ArrayList<String> getNewOutputCols() {
    return newOutputCols;
  }

  public void setNewOutputCols(ArrayList<String> newOutputCols) {
    this.newOutputCols = newOutputCols;
  }

  public Map<String, ExprNodeDesc> getNewColExprMap() {
    return newColExprMap;
  }

  public void setNewColExprMap(Map<String, ExprNodeDesc> newColExprMap) {
    this.newColExprMap = newColExprMap;
  }

  public ArrayList<ExprNodeDesc> getNewColList() {
    return newColList;
  }

  public void setNewColList(ArrayList<ExprNodeDesc> newColList) {
    this.newColList = newColList;
  }

  public ArrayList<ColumnInfo> getNewRS() {
    return newRS;
  }

  public void setNewRS(ArrayList<ColumnInfo> newRS) {
    this.newRS = newRS;
  }

  public RowResolver getNewRR() {
    return newRR;
  }

  public void setNewRR(RowResolver newRR) {
    this.newRR = newRR;
  }

  public List<Operator<? extends Serializable>> getSubqFSParentList() {
    return subqFSParentList;
  }

  public void setSubqFSParentList(List<Operator<? extends Serializable>> subqFSParentList) {
    this.subqFSParentList = subqFSParentList;
  }

  public Operator<? extends Serializable> getSubqSelectOp() {
    return subqSelectOp;
  }

  public void setSubqSelectOp(Operator<? extends Serializable> subqSelectOp) {
    this.subqSelectOp = subqSelectOp;
  }

  public Map<String, String> getAliasToInternal() {
    return aliasToInternal;
  }

  public void setAliasToInternal(Map<String, String> aliasToInternal) {
    this.aliasToInternal = aliasToInternal;
  }


  public ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  public ParseContext getSubqueryPctx() {
    return subqueryPctx;
  }

  public void setSubqueryPctx(ParseContext subqueryPctx) {
    this.subqueryPctx = subqueryPctx;
  }

  public ParseContext getNewDAGCtx() {
    return newDAGCtx;
  }

  public void setNewDAGCtx(ParseContext newDAGCtx) {
    this.newDAGCtx = newDAGCtx;
  }

  public Map<String, ExprNodeDesc> getTsSelColExprMap() {
    return tsSelColExprMap;
  }

  public void setTsSelColExprMap(Map<String, ExprNodeDesc> tsSelColExprMap) {
    this.tsSelColExprMap = tsSelColExprMap;
  }

  public Map<String, ExprNodeDesc> getGbySelColExprMap() {
    return gbySelColExprMap;
  }

  public void setGbySelColExprMap(Map<String, ExprNodeDesc> gbySelColExprMap) {
    this.gbySelColExprMap = gbySelColExprMap;
  }

  public ArrayList<ExprNodeDesc> getGbySelColList() {
    return gbySelColList;
  }

  public void setGbySelColList(ArrayList<ExprNodeDesc> gbySelColList) {
    this.gbySelColList = gbySelColList;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }


  public String getCurrentTableName() {
    return currentTableName;
  }

  public void setCurrentTableName(String currentTableName) {
    this.currentTableName = currentTableName;
  }


  public GenericUDAFEvaluator getEval() {
    return eval;
  }

  public void setEval(GenericUDAFEvaluator eval) {
    this.eval = eval;
  }


  public ParseContextGenerator getPcg() {
    return pcg;
  }



  public Operator<? extends Serializable> getNewTSOp() {
    return newTSOp;
  }

  void createSubqueryContext() {
    String selKeys = "";
    for (String key : indexKeyNames) {
      selKeys += key + ",";
    }
    pcg.setParseContext(parseContext);
    String subqueryCommand = "select " + selKeys + " size(`_offsets`) as CNT from " + indexName;
    subqueryPctx = pcg.generateDAGForSubquery(subqueryCommand);

  }

  public void getAppendSubqueryProc(TableScanOperator origOp){
    List<Operator<? extends Serializable>> origChildrenList = origOp.getChildOperators();


    /* origChildrenList has the child operators for the TableScanOperator of the original DAG
    * We need to get rid of the TS operator of original DAG and append rest of the tree to the sub-query operator DAG
    * This code sets the parentOperators of first operator in origChildrenList to subqFSParentList
    * subqFSParentList contains the parentOperators list of the FileSinkOperator of the sub-query operator DAG
    *
    * subqLastOp is the last SelectOperator of sub-query DAG. The rest of the original operator DAG needs to be appended here
    * Hence, set the subqLastOp's child operators to be origChildrenList
    *
    * */

   if(origChildrenList != null && origChildrenList.size() > 0){
     origChildrenList.get(0).setParentOperators(subqFSParentList);
   }
   if(subqSelectOp != null){
     subqSelectOp.setChildOperators(origChildrenList);
   }


      /* The operator DAG plan is generated in the order FROM-WHERE-GROUPBY-ORDERBY-SELECT
      * We have appended the original operator DAG at the end of the sub-query operator DAG
      *      as the sub-query will always be a part of FROM processing
      *
      * Now we need to insert the final sub-query+original DAG to the original ParseContext
      * parseContext.setOpParseCtx(subqOpOldOpc) sets the subqOpOldOpc OpToParseContext map to the original context
      * parseContext.setTopOps(subqTopMap) sets the topOps map to contain the sub-query topOps map
      * parseContext.setTopToTable(newTopToTable) sets the original topToTable to contain sub-query top TableScanOperator
      */

     HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryPctx.getTopOps();
     Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
     String subqTab = subqTabItr.next();
     Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);

     Table tbl = subqueryPctx.getTopToTable().get(subqOp);
     parseContext.getTopToTable().remove(origOp);
     parseContext.getTopToTable().put((TableScanOperator) subqOp, tbl);

     String tabAlias = "";
     if(currentTableName.contains(":")){
       String[] tabToAlias = currentTableName.split(":");
       if(tabToAlias.length > 1){
         tabAlias = tabToAlias[0] + ":";
       }
     }

     parseContext.getTopOps().remove(currentTableName);
     parseContext.getTopOps().put(tabAlias + subqTab, subqOp);
     newTSOp = subqOp;
     parseContext.getOpParseCtx().remove(origOp);
     parseContext.getOpParseCtx().putAll(subqueryPctx.getOpParseCtx());
     LOG.info("Finished appending subquery");
  }

}
