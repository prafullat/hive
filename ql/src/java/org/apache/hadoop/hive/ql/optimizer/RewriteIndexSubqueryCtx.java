package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * RewriteIndexSubqueryCtx class stores the context for the {@link RewriteIndexSubqueryProcFactory} processor factory methods
 *
 */
public class RewriteIndexSubqueryCtx implements NodeProcessorCtx {
  protected final Log LOG = LogFactory.getLog(RewriteIndexSubqueryCtx.class.getName());

  //This is populated in RewriteIndexSubqueryProcFactory's NewQuerySelectSchemaProc processor with the colExprMap of the
  //SelectOperator whose parent is TableScanOperator
  private Map<String, ExprNodeDesc> newSelColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
  //The next two data structures are populated in RewriteIndexSubqueryProcFactory's NewQuerySelectSchemaProc processor
  //with the colExprMap of the SelectOperator whose child is GroupByOperator
  private ArrayList<ExprNodeDesc> newSelColList = new ArrayList<ExprNodeDesc>();

  // Initialise all data structures required to copy RowResolver, RowSchema, outputColumnNames, colList, colExprMap
  //from subquery DAG to original DAG operators
  private ArrayList<String> newOutputCols = new ArrayList<String>();
  private Map<String, ExprNodeDesc> newColExprMap = new HashMap<String, ExprNodeDesc>();
  private ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
  private ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
  private RowResolver newRR = new RowResolver();

  //This is populated in RewriteIndexSubqueryProcFactory's SubquerySelectSchemaProc processor for later
  //use in NewQuerySelectSchemaProc processor
  private Map<String, String> aliasToInternal = new LinkedHashMap<String, String>();

  // Get the parentOperators List for FileSinkOperator. We need this later to set the
  // parentOperators for original DAG operator
  private List<Operator<? extends Serializable>> subqFSParentList = null;

  // We need the reference to this SelectOperator so that the original DAG can be appended here
  private Operator<? extends Serializable> subqSelectOp = null;

  //We replace the original TS operator with new TS operator from subquery context to scan over the index table
  //rather than the original table
  //private final Operator<? extends Serializable> newTSOp = null;

  private ParseContext parseContext = null;
  private ParseContext subqueryPctx = null;
  private ParseContext newDAGCtx = null;

  private Set<String> indexKeyNames = new LinkedHashSet<String>();
  private String indexName = "";
  private String currentTableName = null;

  //We need the GenericUDAFEvaluator for GenericUDAF function "sum" when we append subquery to original operator tree
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

  public Map<String, ExprNodeDesc> getNewSelColExprMap() {
    return newSelColExprMap;
  }

  public void setNewSelColExprMap(Map<String, ExprNodeDesc> newSelColExprMap) {
    this.newSelColExprMap = newSelColExprMap;
  }

  public ArrayList<ExprNodeDesc> getNewSelColList() {
    return newSelColList;
  }

  public void setNewSelColList(ArrayList<ExprNodeDesc> newSelColList) {
    this.newSelColList = newSelColList;
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

  /*
  public void setNewTSOp(Operator<? extends Serializable> newTSOp) {
    this.newTSOp = newTSOp;
  }

  public Operator<? extends Serializable> getNewTSOp() {
    return newTSOp;
  }
*/
  /**
   * We construct the string command for subquery using index key columns
   * and use the {@link RewriteParseContextGenerator} to generate a operator tree
   * and its ParseContext for the subquery string command
   */
  void createSubqueryContext() {
    String selKeys = "";
    for (String key : indexKeyNames) {
      selKeys += key + ",";
    }
    String subqueryCommand = "select " + selKeys + " size(`_offsets`) as CNT from " + indexName;
    subqueryPctx = RewriteParseContextGenerator.generateOperatorTree(parseContext.getConf(), subqueryCommand);

  }

}
