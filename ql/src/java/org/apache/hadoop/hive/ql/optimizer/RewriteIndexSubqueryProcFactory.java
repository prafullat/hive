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
import org.apache.hadoop.hive.ql.exec.FilterOperator;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public final class RewriteIndexSubqueryProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteIndexSubqueryProcFactory.class.getName());
  private static RewriteIndexSubqueryCtx subqueryCtx = null;

  private RewriteIndexSubqueryProcFactory() {

  }

  public static class SubquerySelectSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      subqueryCtx.getNewOutputCols().clear();
      subqueryCtx.getNewColExprMap().clear();
      subqueryCtx.getNewColList().clear();
      subqueryCtx.getNewRS().clear();
      subqueryCtx.setNewRR(new RowResolver());

      RowResolver oldRR = subqueryCtx.getSubqueryPctx().getOpParseCtx().get(operator).getRowResolver();
      SelectDesc oldConf = (SelectDesc) operator.getConf();
      Map<String, ExprNodeDesc> oldColumnExprMap = operator.getColumnExprMap();
      ArrayList<ExprNodeDesc> oldColList = oldConf.getColList();

      ArrayList<ColumnInfo> schemaSign = operator.getSchema().getSignature();
      for (ColumnInfo columnInfo : schemaSign) {
        String internal = columnInfo.getInternalName();
        String alias = columnInfo.getAlias();
        subqueryCtx.getAliasToInternal().put(alias, internal);
      }


      String internalName = null;
      for(int i=0; i < oldConf.getOutputColumnNames().size(); i++){
        internalName = oldConf.getOutputColumnNames().get(i);
        //Fetch all output columns (required by SelectOperators in original DAG)
        subqueryCtx.getNewOutputCols().add(new String(internalName));

        if(oldColumnExprMap != null){
          ExprNodeDesc end = oldColumnExprMap.get(internalName);
          if(end instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc oldDesc = (ExprNodeColumnDesc)end ;
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) oldDesc.clone();
            newDesc.setColumn(internalName);
            //Fetch columnExprMap (required by SelectOperator and FilterOperator in original DAG)
            subqueryCtx.getNewColExprMap().put(internalName, newDesc);
          }else if(end instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)end ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) {
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              subqueryCtx.getNewColExprMap().put(internalName, newDesc);
            }
          }
        }
        if(oldColList != null){
          ExprNodeDesc exprNodeDesc = oldColList.get(i);
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) exprNodeDesc.clone();
            newDesc.setColumn(internalName);
            //Fetch colList (required by SelectOperators in original DAG)
            subqueryCtx.getNewColList().add(newDesc);
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)exprNodeDesc ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) {
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              subqueryCtx.getNewColList().add(newDesc);
            }

          }
        }


      }

      //We need to set the alias for the new index table subquery
      for (int i = 0; i < subqueryCtx.getNewOutputCols().size(); i++) {
        internalName = subqueryCtx.getNewOutputCols().get(i);
        String[] nm = oldRR.reverseLookup(internalName);
        ColumnInfo col;
        try {
          col = oldRR.get(nm[0], nm[1]);
          if(nm[0] == null){
            nm[0] = "v1";
          }
          // Fetch RowResolver and RowSchema (required by SelectOperator and FilterOperator in original DAG)
          subqueryCtx.getNewRR().put(nm[0], nm[1], col);
          subqueryCtx.getNewRS().add(col);
        } catch (SemanticException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      subqueryCtx.setSubqSelectOp(operator);

      return null;
    }
  }

  public static SubquerySelectSchemaProc getSubquerySelectSchemaProc(){
    return new SubquerySelectSchemaProc();
  }


  public static class SubqueryFileSinkProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FileSinkOperator operator = (FileSinkOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      subqueryCtx.setSubqFSParentList(operator.getParentOperators());
      subqueryCtx.getSubqueryPctx().getOpParseCtx().remove(operator);
      return null;
    }
  }

  public static SubqueryFileSinkProc getSubqueryFileSinkProc(){
    return new SubqueryFileSinkProc();
  }

  public static class AppendSubqueryToOriginalQueryProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator operator = (TableScanOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      List<Operator<? extends Serializable>> origChildrenList = operator.getChildOperators();


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
       origChildrenList.get(0).setParentOperators(subqueryCtx.getSubqFSParentList());
     }
     if(subqueryCtx.getSubqSelectOp() != null){
       subqueryCtx.getSubqSelectOp().setChildOperators(origChildrenList);
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

       HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryCtx.getSubqueryPctx().getTopOps();
       Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
       String subqTab = subqTabItr.next();
       Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);

       Table tbl = subqueryCtx.getSubqueryPctx().getTopToTable().get(subqOp);
       subqueryCtx.getParseContext().getTopToTable().remove(operator);
       subqueryCtx.getParseContext().getTopToTable().put((TableScanOperator) subqOp, tbl);

       String tabAlias = "";
       if(subqueryCtx.getCurrentTableName().contains(":")){
         String[] tabToAlias = subqueryCtx.getCurrentTableName().split(":");
         if(tabToAlias.length > 1){
           tabAlias = tabToAlias[0] + ":";
         }
       }

       subqueryCtx.getParseContext().getTopOps().remove(subqueryCtx.getCurrentTableName());
       subqueryCtx.getParseContext().getTopOps().put(tabAlias + subqTab, subqOp);
       subqueryCtx.setNewTSOp(subqOp);
       subqueryCtx.getParseContext().getOpParseCtx().remove(operator);
       subqueryCtx.getParseContext().getOpParseCtx().putAll(subqueryCtx.getSubqueryPctx().getOpParseCtx());
       LOG.info("Finished appending subquery");
      return null;
    }
  }

  public static AppendSubqueryToOriginalQueryProc getAppendSubqueryToOriginalQueryProc(){
    return new AppendSubqueryToOriginalQueryProc();
  }



  public static class NewQuerySelectSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      List<Operator<? extends Serializable>> parentOps = operator.getParentOperators();
      Operator<? extends Serializable> parentOp = parentOps.iterator().next();
      if(parentOp instanceof TableScanOperator){
        subqueryCtx.setTsSelColExprMap(operator.getColumnExprMap());
      }

      List<Operator<? extends Serializable>> childOps = operator.getChildOperators();
      Operator<? extends Serializable> childOp = childOps.iterator().next();
      SelectDesc selDesc = (SelectDesc) operator.getConf();

      //Copy colList and outputColumns for SelectOperator from sub-query DAG SelectOperator
      if((!(parentOp instanceof TableScanOperator)) && (!(childOp instanceof FileSinkOperator))
                                                    && (!(childOp instanceof ReduceSinkOperator))){
        operator.setColumnExprMap(subqueryCtx.getNewColExprMap());
        subqueryCtx.getParseContext().getOpParseCtx().get(operator).setRowResolver(subqueryCtx.getNewRR());
        operator.getSchema().setSignature(subqueryCtx.getNewRS());
        SelectDesc conf = (SelectDesc) operator.getConf();
        conf.setColList(subqueryCtx.getNewColList());
        conf.setOutputColumnNames(subqueryCtx.getNewOutputCols());
      }

      if (childOp instanceof GroupByOperator){
        subqueryCtx.getGbySelColList().clear();
        subqueryCtx.setGbySelColExprMap(operator.getColumnExprMap());
        /* colExprMap */
        Set<String> internalNamesList = subqueryCtx.getGbySelColExprMap().keySet();
        for (String internal : internalNamesList) {
          ExprNodeDesc end = subqueryCtx.getGbySelColExprMap().get(internal).clone();
          if(end instanceof ExprNodeGenericFuncDesc){
            List<ExprNodeDesc> colExprs = ((ExprNodeGenericFuncDesc)end).getChildExprs();
            for (ExprNodeDesc colExpr : colExprs) {
              if(colExpr instanceof ExprNodeColumnDesc){
                if(!subqueryCtx.getGbySelColList().contains(colExpr)){
                  TypeInfo typeInfo = colExpr.getTypeInfo();
                  if(typeInfo instanceof ListTypeInfo){
                  PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
                  pti.setTypeName("int");
                  colExpr.setTypeInfo(pti);
                  }
                  subqueryCtx.getGbySelColList().add(colExpr);
                }
              }
            }

          }else if(end instanceof ExprNodeColumnDesc){
            if(!subqueryCtx.getGbySelColList().contains(end)){
              subqueryCtx.getGbySelColList().add(end);
            }
          }
        }

        operator.setColumnExprMap(subqueryCtx.getTsSelColExprMap());
        selDesc.setColList(subqueryCtx.getGbySelColList());
      }

      return null;
    }
  }

  public static NewQuerySelectSchemaProc getNewQuerySelectSchemaProc(){
    return new NewQuerySelectSchemaProc();
  }


  public static class NewQueryGroupbySchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator operator = (GroupByOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      if(subqueryCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){

        String table = subqueryCtx.getCurrentTableName();
        if(table.contains(":")){
          String[] aliasAndTab = table.split(":");
          table = aliasAndTab[1];
        }
        String selReplacementCommand = "";
        if(subqueryCtx.getIndexKeyNames().iterator().hasNext()){
          selReplacementCommand = "select sum(" + subqueryCtx.getIndexKeyNames().iterator().next() + ") as TOTAL from " + table
          + " group by " + subqueryCtx.getIndexKeyNames().iterator().next() + " ";
        }
        ParseContext newDAGContext = RewriteParseContextGenerator.generateOperatorTree(subqueryCtx.getParseContext().getConf(),
            selReplacementCommand);
        subqueryCtx.setNewDAGCtx(newDAGContext);
        Map<GroupByOperator, Set<String>> newGbyOpMap = subqueryCtx.getNewDAGCtx().getGroupOpToInputTables();
        GroupByOperator newGbyOperator = newGbyOpMap.keySet().iterator().next();

        GroupByDesc oldConf = operator.getConf();
        ArrayList<AggregationDesc> oldAggrList = oldConf.getAggregators();
        if(oldAggrList != null && oldAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : oldAggrList) {
            if(aggregationDesc != null && aggregationDesc.getGenericUDAFName().equals("count")){
              oldAggrList.remove(aggregationDesc);
              break;
            }

          }
        }

        GroupByDesc newConf = newGbyOperator.getConf();
        ArrayList<AggregationDesc> newAggrList = newConf.getAggregators();
        if(newAggrList != null && newAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : newAggrList) {
            subqueryCtx.setEval(aggregationDesc.getGenericUDAFEvaluator());
            ArrayList<ExprNodeDesc> paraList = aggregationDesc.getParameters();
            for (int i=0; i< paraList.size(); i++) {
              ExprNodeDesc exprNodeDesc = paraList.get(i);
              if(exprNodeDesc instanceof ExprNodeColumnDesc){
                ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
                String col = "cnt";
                if(subqueryCtx.getAliasToInternal().containsKey(col)){
                  encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
                }
                encd.setTabAlias(null);
                exprNodeDesc = encd;
              }
              paraList.set(i, exprNodeDesc);
            }
            oldAggrList.add(aggregationDesc);
          }
        }

        Map<String, ExprNodeDesc> newGbyColExprMap = new LinkedHashMap<String, ExprNodeDesc>();
        Map<String, ExprNodeDesc> oldGbyColExprMap = operator.getColumnExprMap();
        Set<String> internalNameSet = oldGbyColExprMap.keySet();
        for (String internal : internalNameSet) {
          ExprNodeDesc exprNodeDesc  = oldGbyColExprMap.get(internal).clone();
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
            String col = encd.getColumn();
            if(subqueryCtx.getIndexKeyNames().contains(col)){
              encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
            }
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
            List<ExprNodeDesc> colExprs = ((ExprNodeGenericFuncDesc)exprNodeDesc).getChildExprs();
            for (ExprNodeDesc colExpr : colExprs) {
              if(colExpr instanceof ExprNodeColumnDesc){
                ExprNodeColumnDesc encd = (ExprNodeColumnDesc)colExpr;
                String col = encd.getColumn();
                if(subqueryCtx.getIndexKeyNames().contains(col)){
                  encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
                }
              }
            }

          }
          newGbyColExprMap.put(internal, exprNodeDesc);
        }


        ArrayList<ExprNodeDesc> newGbyKeys = new ArrayList<ExprNodeDesc>();
        ArrayList<ExprNodeDesc> oldGbyKeys = oldConf.getKeys();
        for (int i =0; i< oldGbyKeys.size(); i++) {
          ExprNodeDesc exprNodeDesc = oldGbyKeys.get(i).clone();
          if(exprNodeDesc instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
            String col = encd.getColumn();
            if(subqueryCtx.getIndexKeyNames().contains(col)){
              encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
            }
            exprNodeDesc = encd;
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
            ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc)exprNodeDesc;
            List<ExprNodeDesc> colExprs = engfd.getChildExprs();
            for (ExprNodeDesc colExpr : colExprs) {
              if(colExpr instanceof ExprNodeColumnDesc){
                ExprNodeColumnDesc encd = (ExprNodeColumnDesc)colExpr;
                String col = encd.getColumn();
                if(subqueryCtx.getIndexKeyNames().contains(col)){
                  encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
                }

              }
            }
          }
          newGbyKeys.add(exprNodeDesc);
        }

        RowSchema oldRS = operator.getSchema();

        ArrayList<ColumnInfo> oldSign = oldRS.getSignature();
        ArrayList<ColumnInfo> newSign = new ArrayList<ColumnInfo>();
        for (ColumnInfo columnInfo : oldSign) {
          columnInfo.setAlias(null);
          newSign.add(columnInfo);
        }

        oldRS.setSignature(newSign);
        operator.setSchema(oldRS);
        oldConf.setKeys(newGbyKeys);
        oldConf.setAggregators(oldAggrList);
        operator.setColumnExprMap(newGbyColExprMap);
        operator.setConf(oldConf);

      }else{
        GroupByDesc childConf = (GroupByDesc) operator.getConf();
        ArrayList<AggregationDesc> childAggrList = childConf.getAggregators();
        if(childAggrList != null && childAggrList.size() > 0){
          for (AggregationDesc aggregationDesc : childAggrList) {
            aggregationDesc.setGenericUDAFEvaluator(subqueryCtx.getEval());
            aggregationDesc.setGenericUDAFName("sum");
          }
        }

      }

      return null;
    }
  }

  public static NewQueryGroupbySchemaProc getNewQueryGroupbySchemaProc(){
    return new NewQueryGroupbySchemaProc();
  }


  public static class NewQueryFilterSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator operator = (FilterOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      operator.getSchema().setSignature(subqueryCtx.getNewRS());
      subqueryCtx.getParseContext().getOpParseCtx().get(operator).setRowResolver(subqueryCtx.getNewRR());

      FilterDesc conf = operator.getConf();
      ExprNodeDesc exprNodeDesc = conf.getPredicate();
      setFilterPredicateCol(exprNodeDesc);
      conf.setPredicate(exprNodeDesc);
      return null;
    }
  }


  private static void setFilterPredicateCol(ExprNodeDesc exprNodeDesc){
    if(exprNodeDesc instanceof ExprNodeColumnDesc){
      ExprNodeColumnDesc encd = (ExprNodeColumnDesc)exprNodeDesc;
      String col = encd.getColumn();
      if(subqueryCtx.getIndexKeyNames().contains(col)){
        encd.setColumn(subqueryCtx.getAliasToInternal().get(col));
      }
      exprNodeDesc = encd;
    }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){
      ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc)exprNodeDesc;
      List<ExprNodeDesc> colExprs = engfd.getChildExprs();
      for (ExprNodeDesc colExpr : colExprs) {
        setFilterPredicateCol(colExpr);
      }
    }

  }


  public static NewQueryFilterSchemaProc getNewQueryFilterSchemaProc(){
    return new NewQueryFilterSchemaProc();
  }


}
