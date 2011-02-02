package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Factory of processors used by {@link RewriteGBUsingIndex} (see invokeRemoveGbyProc(..) method)
 * Each of the processors are invoked according to a rule and serve towards removing
 * group-by construct from original operator tree
 *
 */
public final class RewriteRemoveGroupbyProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteRemoveGroupbyProcFactory.class.getName());
  private static RewriteRemoveGroupbyCtx removeGbyCtx = null;

  private RewriteRemoveGroupbyProcFactory() {
    //this prevents the class from getting instantiated
  }

  /**
   * This processor removes the SelectOperator whose child is a GroupByOperator from the operator tree (OpParseContext).
   * When we remove the group-by construct from the query, we do not need this SelectOperator which worked initially as an
   * interim operator to pass arguments from the parent TableScanOperator to the child GroupByOperator (Remember that the genPlan(..)
   * method creates the operators bottom-up FROM-WHERE-GROUPBY-ORDER-BY-SELECT etc)
   *
   * Since we need to remove the group-by construct (comprising of GBY-RS-GBY operators and interim SEL operator), the processor sets the
   * appropriate parent-child links.
   *
   * The processor also constructs a ExprNodeDesc instance for the size(_offsets) function and replaces the index key columns
   * with this function descriptor. It also sets the rowSchema, colList and colExprMap data structures correctly for this SelectOperator
   * to accommodate the new replacement and removal of group-by construct
   *
   */
  private static class ReplaceIdxKeyWithSizeFunc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      removeGbyCtx = (RewriteRemoveGroupbyCtx)ctx;

      //as of now, we have hard-coded the positions as get(0) etc as whenever a group-by construct appears in teh operator tree,
      //it comes in the SEL-GBY-RS-SEL combination. This lets us presume that the parent or child operator will always be
      // at the 0th position in the DAG operator tree
      List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
      Operator<? extends Serializable> child = childrenList.get(0);
      Operator<? extends Serializable> parent = operator.getParentOperators().get(0);

      if(child instanceof GroupByOperator){
        //this is the interim SEL operator for the group-by construct, we do not need this in the re-written operator tree
        removeGbyCtx.getNewParentList().addAll(operator.getParentOperators());
        removeGbyCtx.getOpc().remove(operator);
      }else if(parent instanceof GroupByOperator){

        // set the child operator list of interim SEL's parent operator to be the child operator list of the GroupByOperator
        removeGbyCtx.getNewParentList().get(0).setChildOperators(removeGbyCtx.getNewChildrenList());
        // set the parent operator list for the SelectOperator (whose parent operator is GroupByOperator)
        //to be the parent list of interim SEL operator
        removeGbyCtx.getNewChildrenList().get(0).setParentOperators(removeGbyCtx.getNewParentList());

        //This code parses the string command and constructs a  ASTNode parse tree
        //we need this to construct the ExprNodeDesc for the size(_offsets) function
        HiveConf conf = removeGbyCtx.getParseContext().getConf();
        Context context = null;
        ASTNode tree = null;
        BaseSemanticAnalyzer sem = null;
        String newSelCommand = "select size(`_offsets`) from " + removeGbyCtx.getIndexName();
        try {
          context = new Context(conf);
        ParseDriver pd = new ParseDriver();
        tree = pd.parse(newSelCommand, context);
        tree = ParseUtils.findRootNonNullToken(tree);
        sem = SemanticAnalyzerFactory.get(conf, tree);

        } catch (ParseException e) {
          LOG.info("ParseException in ReplaceIdxKeyWithSizeFunc");
          e.printStackTrace();
        } catch (SemanticException e) {
          LOG.info("SemanticException in ReplaceIdxKeyWithSizeFunc");
          e.printStackTrace();
        } catch (IOException e) {
          LOG.info("IOException in ReplaceIdxKeyWithSizeFunc");
          e.printStackTrace();
        }

        //We retrieve the ASTNode function token from the root tree
        ASTNode funcNode = removeGbyCtx.getFuncNode(tree, "size");

        //We need the rowResolver of the parent TableScanOperator to fix the rowSchema, colList, colExprMap of the SelectOperator
        //and also to construct the  ExprNodeDesc to replace the index key columns with size(_offsets) GenericUDF
        LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opCtxMap =
          removeGbyCtx.getParseContext().getOpParseCtx();
        Operator<? extends Serializable> tsOp = removeGbyCtx.getTopOperator(operator);
        OpParseContext tsCtx = opCtxMap.get(tsOp);
        ExprNodeDesc exprNode = ((SemanticAnalyzer) sem).genExprNodeDesc(funcNode, tsCtx.getRowResolver());

        //We need the name of the GenericUDF function to correct the rowSchema
        String funcName = "";

        if(exprNode instanceof ExprNodeGenericFuncDesc){
          List<ExprNodeDesc> exprList = ((ExprNodeGenericFuncDesc) exprNode).getChildExprs();
          for (ExprNodeDesc exprNodeDesc : exprList) {
            if(exprNodeDesc instanceof ExprNodeColumnDesc){
              funcName = ((ExprNodeColumnDesc) exprNodeDesc).getColumn();
            }
          }
        }

        SelectDesc selDesc = (SelectDesc) operator.getConf();
        //Since we have removed the interim SEL operator when we removed the group-by construct, we need to get rid
        //of the internal names in the colList and colExprMap of this SelectOperator
        //internalToAlias map gives us this mapping to correct these data structures
        HashMap<String, String> internalToAlias = new LinkedHashMap<String, String>();

        //Set the new RowSchema and populate the internalToAlias map
        RowSchema rs = operator.getSchema();
        ArrayList<ColumnInfo> newRS = new ArrayList<ColumnInfo>();
        ArrayList<ColumnInfo> sign = rs.getSignature();
        for (ColumnInfo columnInfo : sign) {
          String alias = columnInfo.getAlias();
          String internalName = columnInfo.getInternalName();
          internalToAlias.put(internalName, alias);
          //the function name always has alias starting with _c (for eg. _c1 etc)
          //We need to set the new alias (_offsets) for the initial "_c1" in rowSchema
          if(alias != null && alias.startsWith("_c")){
            columnInfo.setAlias(funcName);
          }
          newRS.add(columnInfo);
        }
        operator.getSchema().setSignature(newRS);

        //Set the colList of this SelectOperator
        ArrayList<ExprNodeDesc> colList = selDesc.getColList();
        int i = 0;
        for (; i< colList.size(); i++) {
          ExprNodeDesc exprNodeDesc = colList.get(i);
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            String internal = ((ExprNodeColumnDesc)exprNodeDesc).getColumn();
            //get rid of the internal column names like _col0, _col1 and replace them with their actual names i.e. alias
            if(internalToAlias.get(internal) != null){
              ((ExprNodeColumnDesc) exprNodeDesc).setColumn(internalToAlias.get(internal));
            }
            //however, if the alias itself is the internal name of the function argument, say _c1, we need to replace the
            //ExprNodeColumnDesc instance with the ExprNodeGenericFuncDesc (i.e. exprNode here)
            //this replaces the count(literal) or count(index_key) function with size(_offsets)
            if(((ExprNodeColumnDesc) exprNodeDesc).getColumn().startsWith("_c")){
              colList.set(i, exprNode);
            }
          }
        }

        selDesc.setColList(colList);

        //Set the new colExprMap for this SelectOperator
        Map<String, ExprNodeDesc> origColExprMap = operator.getColumnExprMap();
        Map<String, ExprNodeDesc> newColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
        Set<String> internalNamesList = origColExprMap.keySet();
        for (String internal : internalNamesList) {
          ExprNodeDesc end = origColExprMap.get(internal).clone();
          if(end instanceof ExprNodeColumnDesc){
            //get rid of the internal column names like _col0, _col1 and replace them with their actual names i.e. alias
            if(internalToAlias.get(internal) != null){
              ((ExprNodeColumnDesc) end).setColumn(internalToAlias.get(internal));
            }
            //this replaces the count(literal) or count(index_key) function with size(_offsets)
            if(((ExprNodeColumnDesc) end).getColumn().startsWith("_c")){
              newColExprMap.put(internal, exprNode);
            }else{
              newColExprMap.put(internal, end);
            }
          }else{
            newColExprMap.put(internal, end);
          }
        }
        operator.setColumnExprMap(newColExprMap);
      }
      return null;
    }
  }

    public static ReplaceIdxKeyWithSizeFunc getReplaceIdxKeyWithSizeFuncProc(){
    return new ReplaceIdxKeyWithSizeFunc();
  }


  /**
   * This processor replaces the original TableScanOperator with the new TableScanOperator and metadata that scans over the
   * index table rather than scanning over the orginal table.
   *
   */
  private static class RepaceTableScanOpProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOperator = (TableScanOperator)nd;
      removeGbyCtx = (RewriteRemoveGroupbyCtx)ctx;

      HashMap<TableScanOperator, Table>  topToTable =
        removeGbyCtx.getParseContext().getTopToTable();

      //construct a new descriptor for the index table scan
      TableScanDesc indexTableScanDesc = new TableScanDesc();
      indexTableScanDesc.setGatherStats(false);

      //String tableName = removeGbyCtx.getCanApplyCtx().findBaseTable(baseTableName);
      String tableName = removeGbyCtx.getIndexName();

      tableSpec ts = new tableSpec(removeGbyCtx.getHiveDb(),
          removeGbyCtx.getParseContext().getConf(),
          tableName
      );
      String k = tableName + Path.SEPARATOR;
      indexTableScanDesc.setStatsAggPrefix(k);
      scanOperator.setConf(indexTableScanDesc);

      //remove original TableScanOperator
      topToTable.clear();
      removeGbyCtx.getParseContext().getTopOps().clear();

      //Scan operator now points to other table
      scanOperator.setAlias(tableName);
      topToTable.put(scanOperator, ts.tableHandle);
      removeGbyCtx.getParseContext().setTopToTable(topToTable);

      OpParseContext operatorContext =
        removeGbyCtx.getParseContext().getOpParseCtx().get(scanOperator);
      RowResolver rr = new RowResolver();
      removeGbyCtx.getParseContext().getOpParseCtx().remove(scanOperator);


      //Construct the new RowResolver for the new TableScanOperator
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) ts.tableHandle.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
        .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          rr.put(tableName, fields.get(i).getFieldName(), new ColumnInfo(fields
              .get(i).getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), tableName, false));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      //Set row resolver for new table
      operatorContext.setRowResolver(rr);

      //Put the new TableScanOperator in the OpParseContext and topOps maps of the original ParseContext
      removeGbyCtx.getParseContext().getOpParseCtx().put(scanOperator, operatorContext);
      removeGbyCtx.getParseContext().getTopOps().put(tableName, scanOperator);
      return null;
    }
  }

  public static RepaceTableScanOpProc getReplaceTableScanProc(){
    return new RepaceTableScanOpProc();
  }

  /**
   * This processor removes the GroupBy operators and the interim ReduceSinkOperator from the OpParseContext
   *
   */
  private static class RemoveGBYProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator operator = (GroupByOperator)nd;
      removeGbyCtx = (RewriteRemoveGroupbyCtx)ctx;
      //On walking the operator tree using the rule 'GBY-RS-GBY', we get the GroupByOperator that is not in the 'groupOpToInputTables'
      //map in the ParseContext. Hence the check.
      if(!removeGbyCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){
        removeGbyCtx.getNewChildrenList().addAll(operator.getChildOperators());

        ReduceSinkOperator rsOp = (ReduceSinkOperator) operator.getParentOperators().get(0);
        removeGbyCtx.getOpc().remove(rsOp);

        GroupByOperator gbyOp = (GroupByOperator) rsOp.getParentOperators().get(0);
        //we need to remove this GBY operator from the groupOpToInputTables map from ParseContext as well
        removeGbyCtx.getParseContext().getGroupOpToInputTables().remove(gbyOp);
        removeGbyCtx.getOpc().remove(gbyOp);

      }

    return null;
    }
  }

  public static RemoveGBYProc getRemoveGroupByProc(){
    return new RemoveGBYProc();
  }


}
