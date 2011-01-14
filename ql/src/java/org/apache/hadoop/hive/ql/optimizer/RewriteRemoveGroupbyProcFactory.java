package org.apache.hadoop.hive.ql.optimizer;

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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public final class RewriteRemoveGroupbyProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteRemoveGroupbyProcFactory.class.getName());
  private static RewriteRemoveGroupbyCtx removeGbyCtx = null;

  private RewriteRemoveGroupbyProcFactory() {

  }

  public static class ReplaceIdxKeyWithSizeFunc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      removeGbyCtx = (RewriteRemoveGroupbyCtx)ctx;
      List<Operator<? extends Serializable>> childrenList = operator.getChildOperators();
      Operator<? extends Serializable> child = childrenList.get(0);
      Operator<? extends Serializable> parent = operator.getParentOperators().get(0);
      if(child instanceof GroupByOperator){
        removeGbyCtx.setNewParentList(operator.getParentOperators());
        removeGbyCtx.getOpc().remove(operator);
      }else if(!(parent instanceof SelectOperator)){
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
            if(internalToAlias.get(internal) != null){
              ((ExprNodeColumnDesc) end).setColumn(internalToAlias.get(internal));
            }
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
            if(internalToAlias.get(internal) != null){
              ((ExprNodeColumnDesc) exprNodeDesc).setColumn(internalToAlias.get(internal));
            }
            newColList.add(exprNodeDesc);
          }
        }
        selDesc.setColList(newColList);

     }

      return null;
    }
  }

  public static ReplaceIdxKeyWithSizeFunc getReplaceIdxKeyWithSizeFuncProc(){
    return new ReplaceIdxKeyWithSizeFunc();
  }

  public static class RepaceTableScanOpProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOperator = (TableScanOperator)nd;

      removeGbyCtx.getNewParentList().get(0).setChildOperators(removeGbyCtx.getNewChildrenList());
      removeGbyCtx.getNewChildrenList().get(0).setParentOperators(removeGbyCtx.getNewParentList());

      HashMap<TableScanOperator, Table>  topToTable =
        removeGbyCtx.getParseContext().getTopToTable();


      String baseTableName = topToTable.get(scanOperator).getTableName();
      if( removeGbyCtx.getCanApplyCtx().findBaseTable(baseTableName) == null ) {
        LOG.debug("No mapping found for original table and index table name");
      }

      //Get the lineage information corresponding to this
      //and modify it ?
      TableScanDesc indexTableScanDesc = new TableScanDesc();
      indexTableScanDesc.setGatherStats(false);

      String tableName = removeGbyCtx.getCanApplyCtx().findBaseTable(baseTableName);

      tableSpec ts = new tableSpec(removeGbyCtx.getHiveDb(),
          removeGbyCtx.getParseContext().getConf(),
          tableName
      );
      String k = tableName + Path.SEPARATOR;
      indexTableScanDesc.setStatsAggPrefix(k);
      scanOperator.setConf(indexTableScanDesc);

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
      removeGbyCtx.getParseContext().getOpParseCtx().put(scanOperator, operatorContext);
      removeGbyCtx.getParseContext().getTopOps().put(tableName, scanOperator);
      return null;
    }
  }

  public static RepaceTableScanOpProc getReplaceTableScanProc(){
    return new RepaceTableScanOpProc();
  }

  public static class RemoveGBYProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator operator = (GroupByOperator)nd;
      removeGbyCtx = (RewriteRemoveGroupbyCtx)ctx;
      if(!removeGbyCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){
        removeGbyCtx.setNewChildrenList(operator.getChildOperators());

        ReduceSinkOperator rsOp = (ReduceSinkOperator) operator.getParentOperators().get(0);
        removeGbyCtx.getOpc().remove(rsOp);

        GroupByOperator gbyOp = (GroupByOperator) rsOp.getParentOperators().get(0);
        removeGbyCtx.getParseContext().getGroupOpToInputTables().remove(gbyOp);
        removeGbyCtx.getOpc().remove(gbyOp);

      }
      removeGbyCtx.getOpc().remove(operator);
      return null;
    }
  }

  public static RemoveGBYProc getRemoveGroupByProc(){
    return new RemoveGBYProc();
  }


}
