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

/**
 * Factory of processors used in {@link RewriteGBUsingIndex} (see invokeSubquerySelectSchemaProc(..) method)
 * Each of the processors are invoked according to a rule and serve to append subquery to original operator tree.
 *
 * This subquery scans over the index table rather than the original table.
 * IT replaces the count(literal)/count(index_key) function in the original select operator
 * with sum(cnt) where cnt is size(_offsets) from subquery select operator.
 *
 * This change necessitates change in the rowSchema, colList, colExprMap, rowResolver of all the SelectOperator's in original
 * operator tree. It also requires to set appropriate predicate parameters and group-by aggregation parameters in original
 * operator tree. Each of the processors in this Factory take care of these changes.
 *
 */
public final class RewriteIndexSubqueryProcFactory {
  protected final static Log LOG = LogFactory.getLog(RewriteIndexSubqueryProcFactory.class.getName());
  private static RewriteIndexSubqueryCtx subqueryCtx = null;

  private RewriteIndexSubqueryProcFactory() {
    //this prevents the class from getting instantiated
  }

  /**
   * This processor retrieves the rowSchema, rowResolver, colList, colExprMap and outputColumnNames data structures
   * from the SelectOperator and its descriptor(SelectDesc). It stores the information in the RewriteIndexSubqueryCtx instance
   * for later use in correcting the schema of original operator tree.
   *
   */
  private static class SubquerySelectSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      //We need to clear this every time in cases where there are multiple operator tree paths with multiple SelectOperators
      subqueryCtx.getNewOutputCols().clear();
      subqueryCtx.getNewColExprMap().clear();
      subqueryCtx.getNewColList().clear();
      subqueryCtx.getNewRS().clear();
      subqueryCtx.setNewRR(new RowResolver());


      RowResolver oldRR = subqueryCtx.getSubqueryPctx().getOpParseCtx().get(operator).getRowResolver();
      SelectDesc oldConf = (SelectDesc) operator.getConf();
      Map<String, ExprNodeDesc> oldColumnExprMap = operator.getColumnExprMap();
      ArrayList<ExprNodeDesc> oldColList = oldConf.getColList();

       //We create the mapping of column name alias to internal name for later use in correcting original operator tree
      ArrayList<ColumnInfo> schemaSign = operator.getSchema().getSignature();
      for (ColumnInfo columnInfo : schemaSign) {
        String internal = columnInfo.getInternalName();
        String alias = columnInfo.getAlias();
        subqueryCtx.getAliasToInternal().put(alias, internal);
      }

      /**outputColumnNames**/
      String internalName = null;
      for(int i=0; i < oldConf.getOutputColumnNames().size(); i++){
        internalName = oldConf.getOutputColumnNames().get(i);
        //Populate all output columns (required by SelectOperators in original DAG) in RewriteIndexSubqueryCtx
        subqueryCtx.getNewOutputCols().add(new String(internalName));

        /**colExprMap**/
        if(oldColumnExprMap != null){
          ExprNodeDesc end = oldColumnExprMap.get(internalName); //in case of simple column names
          if(end instanceof ExprNodeColumnDesc){
            ExprNodeColumnDesc oldDesc = (ExprNodeColumnDesc)end ;
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) oldDesc.clone();
            newDesc.setColumn(internalName);
            //Populate columnExprMap (required by SelectOperator and FilterOperator in original DAG) in RewriteIndexSubqueryCtx
            subqueryCtx.getNewColExprMap().put(internalName, newDesc);
          }else if(end instanceof ExprNodeGenericFuncDesc){ //in case of functions on columns
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)end ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) { //we have the list of columns here
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              //Populate columnExprMap (required by SelectOperator and FilterOperator in original DAG) in RewriteIndexSubqueryCtx
              subqueryCtx.getNewColExprMap().put(internalName, newDesc);
            }
          }
        }

        /**colList**/
        if(oldColList != null){
          ExprNodeDesc exprNodeDesc = oldColList.get(i);
          if(exprNodeDesc instanceof ExprNodeColumnDesc){//in case of simple column names
            ExprNodeColumnDesc newDesc = (ExprNodeColumnDesc) exprNodeDesc.clone();
            newDesc.setColumn(internalName);
            //Populate colList (required by SelectOperators in original DAG) in RewriteIndexSubqueryCtx
            subqueryCtx.getNewColList().add(newDesc);
          }else if(exprNodeDesc instanceof ExprNodeGenericFuncDesc){//in case of functions on columns
            ExprNodeGenericFuncDesc oldDesc = (ExprNodeGenericFuncDesc)exprNodeDesc ;
            ExprNodeGenericFuncDesc newDesc = (ExprNodeGenericFuncDesc) oldDesc.clone();
            List<ExprNodeDesc> childExprs = newDesc.getChildExprs();
            List<ExprNodeDesc> newChildExprs = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc childEnd : childExprs) {//we have the list of columns here
              if(childEnd instanceof ExprNodeColumnDesc){
                ((ExprNodeColumnDesc) childEnd).setColumn(internalName);
                newChildExprs.add(childEnd);
              }
              newDesc.setChildExprs(newChildExprs);
              //Populate colList (required by SelectOperators in original DAG) in RewriteIndexSubqueryCtx
              subqueryCtx.getNewColList().add(newDesc);
            }
          }
        }
      }

      /**RowSchema and RowResolver**/
      for (int i = 0; i < subqueryCtx.getNewOutputCols().size(); i++) {
        internalName = subqueryCtx.getNewOutputCols().get(i);
        String[] nm = oldRR.reverseLookup(internalName);
        ColumnInfo col;
        try {
          //We need to set the alias for the new index table subquery
          col = oldRR.get(nm[0], nm[1]);
          if(nm[0] == null){
            nm[0] = "v" + i; //add different alias in case original query has multiple subqueries
          }
          // Populate RowResolver and RowSchema (required by SelectOperator and FilterOperator in original DAG) in RewriteIndexSubqueryCtx
          subqueryCtx.getNewRR().put(nm[0], nm[1], col);
          subqueryCtx.getNewRS().add(col);
        } catch (SemanticException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      //We need this SelectOperator from subquery as a reference point to append in original query
      subqueryCtx.setSubqSelectOp(operator);

      return null;
    }
  }

  public static SubquerySelectSchemaProc getSubquerySelectSchemaProc(){
    return new SubquerySelectSchemaProc();
  }


  /**
   * We do not need the fileSinkOperator of the subquery operator tree when we append the rest of the subquery operator tree
   * to the original operator tree. This processor gets rid of this FS operator by removing it from subquery OpParseContext.
   *
   */
  private static class SubqueryFileSinkProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FileSinkOperator operator = (FileSinkOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      //Store the list of FileSinkOperator's parent operators as we later append the original query
      //at the end of the subquery operator tree (without the FileSinkOperator).
      subqueryCtx.setSubqFSParentList(operator.getParentOperators());
      subqueryCtx.getSubqueryPctx().getOpParseCtx().remove(operator);
      return null;
    }
  }

  public static SubqueryFileSinkProc getSubqueryFileSinkProc(){
    return new SubqueryFileSinkProc();
  }

  /**
   * This processor appends the subquery operator tree to the original operator tree.
   * Since genPlan(..) method from the SemanticAnalyzer creates the operator tree bottom-up i.e.
   * FROM-WHERE-GROUPBY-ORDERBY-SELECT etc, any query with nested subqueries will have the TableScanOperator of the
   * innermost subquery as the top operator in the topOps and topToTable maps.
   *
   * Any subquery which is a part of the from clause
   * (eg: SELECT * FROM (SELECT DISTINCT key, value FROM tbl) v1 WHERE v1.value = 2;) always has its
   * DAG operator tree appended before the operator tree of the enclosing query.
   * For example, for the above query, the operator tree is:
   * <TS(0)[subq]--->SEL(1)[subq]--->GBY(2)[subq]--->RS(3)[subq]--->GBY(4)[subq]--->SEL(5)[subq]--->FIL(6)[orig]--->SEL(7)[orig]--->FS(8)[orig]>
   *
   * We replace the TableScanOperator (TS) of the original operator tree with the whole subquery operator tree (without the
   * FileSinkOperator of the subquery operator tree).
   *
   */
  private static class AppendSubqueryToOriginalQueryProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator operator = (TableScanOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      List<Operator<? extends Serializable>> origChildrenList = operator.getChildOperators();

      /* origChildrenList has the child operators for the TableScanOperator of the original DAG
      * We need to get rid of the TS operator of original DAG and append rest of the tree to the sub-query operator DAG
      * This code sets the parentOperators of first operator in origChildrenList to subqFSParentList.
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
      * Now we need to insert the final sub-query+original DAG to the original ParseContext
      */

       HashMap<String, Operator<? extends Serializable>> subqTopMap = subqueryCtx.getSubqueryPctx().getTopOps();
       Iterator<String> subqTabItr = subqTopMap.keySet().iterator();
       String subqTab = subqTabItr.next();
       Operator<? extends Serializable> subqOp = subqTopMap.get(subqTab);
       Table tbl = subqueryCtx.getSubqueryPctx().getTopToTable().get(subqOp);

       //remove original TableScanOperator from the topToTable map
       //Put the new TableScanOperator (top operator of the subquery operator tree) to topToTable map
       subqueryCtx.getParseContext().getTopToTable().remove(operator);
       subqueryCtx.getParseContext().getTopToTable().put((TableScanOperator) subqOp, tbl);

       String tabAlias = "";
       if(subqueryCtx.getCurrentTableName().contains(":")){
         String[] tabToAlias = subqueryCtx.getCurrentTableName().split(":");
         if(tabToAlias.length > 1){
           tabAlias = tabToAlias[0] + ":";
         }
       }
       //remove original table and operator tree mapping from topOps
       //put the new table alias adn subquery index table as the key and the new operator tree as value in topOps
       subqueryCtx.getParseContext().getTopOps().remove(subqueryCtx.getCurrentTableName());
       subqueryCtx.getParseContext().getTopOps().put(tabAlias + subqTab, subqOp);

       //we need this later
       //subqueryCtx.setNewTSOp(subqOp);

       //remove original TableScanOperator from the original OpParsecontext
       //add all values from the subquery OpParseContext to the original OpParseContext
       subqueryCtx.getParseContext().getOpParseCtx().remove(operator);
       subqueryCtx.getParseContext().getOpParseCtx().putAll(subqueryCtx.getSubqueryPctx().getOpParseCtx());
       LOG.info("Finished appending subquery");
      return null;
    }
  }

  public static AppendSubqueryToOriginalQueryProc getAppendSubqueryToOriginalQueryProc(){
    return new AppendSubqueryToOriginalQueryProc();
  }



  /**
   * NewQuerySelectSchemaProc.
   *
   */
  private static class NewQuerySelectSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator operator = (SelectOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      List<Operator<? extends Serializable>> parentOps = operator.getParentOperators();
      Operator<? extends Serializable> parentOp = parentOps.iterator().next();
      List<Operator<? extends Serializable>> childOps = operator.getChildOperators();
      Operator<? extends Serializable> childOp = childOps.iterator().next();


      if(parentOp instanceof TableScanOperator){
        //We need to copy the colExprMap of this SelectOperator whose parent is TableScanOperator to the
        //colExprMap of the SelectOperator whose child operator is a GroupByOperator
        subqueryCtx.setNewSelColExprMap(operator.getColumnExprMap());
      }else if((!(parentOp instanceof TableScanOperator)) //skip first SelectOperator in operator tree
          && (!(childOp instanceof FileSinkOperator))     //skip last SelectOperator in operator tree
          && (!(childOp instanceof ReduceSinkOperator))){ //skip the SelectOperator which appears before a JOIN in operator tree

        //Copy colList and outputColumns for SelectOperator from sub-query DAG SelectOperator
        //these are all the SelectOperators that come in between the first SelectOperator and last SelectOperator in the operator tree
        operator.setColumnExprMap(subqueryCtx.getNewColExprMap());
        subqueryCtx.getParseContext().getOpParseCtx().get(operator).setRowResolver(subqueryCtx.getNewRR());
        operator.getSchema().setSignature(subqueryCtx.getNewRS());
        SelectDesc conf = (SelectDesc) operator.getConf();
        conf.setColList(subqueryCtx.getNewColList());
        conf.setOutputColumnNames(subqueryCtx.getNewOutputCols());
      }

      if (childOp instanceof GroupByOperator){
        //use the original columnExprMap to construct the newColList
        subqueryCtx.getNewSelColList().clear();
        /**colList**/
        Set<String> internalNamesList = operator.getColumnExprMap().keySet();
        for (String internal : internalNamesList) {
          ExprNodeDesc end = operator.getColumnExprMap().get(internal).clone();
          if(end instanceof ExprNodeGenericFuncDesc){
            List<ExprNodeDesc> colExprs = ((ExprNodeGenericFuncDesc)end).getChildExprs();
            for (ExprNodeDesc colExpr : colExprs) {
              if(colExpr instanceof ExprNodeColumnDesc){
                if(!subqueryCtx.getNewSelColList().contains(colExpr)){
                  TypeInfo typeInfo = colExpr.getTypeInfo();
                  if(typeInfo instanceof ListTypeInfo){
                    PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
                    pti.setTypeName("int");
                    colExpr.setTypeInfo(pti);
                  }
                  subqueryCtx.getNewSelColList().add(colExpr);
                }
              }
            }

          }else if(end instanceof ExprNodeColumnDesc){
            if(!subqueryCtx.getNewSelColList().contains(end)){
              subqueryCtx.getNewSelColList().add(end);
            }
          }
        }
        //Set the new colExprMap and new colList
        operator.setColumnExprMap(subqueryCtx.getNewSelColExprMap());
        SelectDesc selDesc = (SelectDesc) operator.getConf();
        selDesc.setColList(subqueryCtx.getNewSelColList());
      }

      return null;
    }
  }

  public static NewQuerySelectSchemaProc getNewQuerySelectSchemaProc(){
    return new NewQuerySelectSchemaProc();
  }


  /**
   * We need to replace the count(literal) GenericUDAF aggregation function for group-by construct to "sum" GenericUDAF.
   * This processor creates a new operator tree for a sample query that creates a GroupByOperator with sum aggregation function
   * and uses that GroupByOperator information to replace the original GroupByOperator aggregation information.
   * It replaces the AggregationDesc (aggregation descriptor) of the old GroupByOperator with the new Aggregation Desc
   * of the new GroupByOperator.
   *
   * The processor also corrects the RowSchema and group-by keys by replacing the existing internal names with the new internal names.
   * This change is required as we add a new subquery to the original query which triggers this change.
   *
   */
  private static class NewQueryGroupbySchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator operator = (GroupByOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;

      //We need to replace the GroupByOperator which is in groupOpToInputTables map with the new GroupByOperator
      if(subqueryCtx.getParseContext().getGroupOpToInputTables().containsKey(operator)){
        //we need to get rif of the alias and construct a query only with the base table name
        String table = subqueryCtx.getCurrentTableName();
        if(table.contains(":")){
          String[] aliasAndTab = table.split(":");
          table = aliasAndTab[1];
        }
        String selReplacementCommand = "";
        if(subqueryCtx.getIndexKeyNames().iterator().hasNext()){
          //the query contains the sum aggregation GenericUDAF
          selReplacementCommand = "select sum(" + subqueryCtx.getIndexKeyNames().iterator().next() + ") as TOTAL from " + table
          + " group by " + subqueryCtx.getIndexKeyNames().iterator().next() + " ";
        }
        //create a new ParseContext for the query to retrieve its operator tree, and the required GroupByOperator from it
        ParseContext newDAGContext = RewriteParseContextGenerator.generateOperatorTree(subqueryCtx.getParseContext().getConf(),
            selReplacementCommand);
        subqueryCtx.setNewDAGCtx(newDAGContext);

        //we get our new GroupByOperator here
        Map<GroupByOperator, Set<String>> newGbyOpMap = subqueryCtx.getNewDAGCtx().getGroupOpToInputTables();
        GroupByOperator newGbyOperator = newGbyOpMap.keySet().iterator().next();

        //remove the old GroupByOperator
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

        //Construct the new AggregationDesc to get rid of the current internal names and replace them with new internal names
        //as required by the operator tree
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

        //Construct the new colExprMap to get rid of the current internal names and replace them with new internal names
        //as required by the operator tree
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

        //Construct the new group-by keys to get rid of the current internal names and replace them with new internal names
        //as required by the operator tree
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

        //Construct the new RowSchema. We do not need a alias for the new internalNames
        RowSchema oldRS = operator.getSchema();
        ArrayList<ColumnInfo> oldSign = oldRS.getSignature();
        ArrayList<ColumnInfo> newSign = new ArrayList<ColumnInfo>();
        for (ColumnInfo columnInfo : oldSign) {
          columnInfo.setAlias(null);
          newSign.add(columnInfo);
        }

        //reset the above data structures in the original GroupByOperator
        oldRS.setSignature(newSign);
        operator.setSchema(oldRS);
        oldConf.setKeys(newGbyKeys);
        oldConf.setAggregators(oldAggrList);
        operator.setColumnExprMap(newGbyColExprMap);
        operator.setConf(oldConf);

      }else{
        //we just need to reset the GenericUDAFEvaluator and its name for this GroupByOperator whose parent is the
        //ReduceSinkOperator
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


  /**
   * This processor corrects the RowResolver for the FilterOperator of the original operator tree using
   * the RowResolver obtained from the subquery SelectOperator in SubquerySelectSchemaProc processor.
   * It also needs to replace the current internal names with new internal names for all instances of the
   * ExprNodeColumnDesc. It recursively calls the setFilterPredicateCol(..) method to set this information correctly.
   *
   */
  private static class NewQueryFilterSchemaProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator operator = (FilterOperator)nd;
      subqueryCtx = (RewriteIndexSubqueryCtx)ctx;
      //Set new RowResolver
      operator.getSchema().setSignature(subqueryCtx.getNewRS());
      subqueryCtx.getParseContext().getOpParseCtx().get(operator).setRowResolver(subqueryCtx.getNewRR());

      //Set correct internalNames
      FilterDesc conf = operator.getConf();
      ExprNodeDesc exprNodeDesc = conf.getPredicate();
      setFilterPredicateCol(exprNodeDesc);
      conf.setPredicate(exprNodeDesc);
      return null;
    }
  }


  /**
   * This method is recursively called whenever we have our expression node descriptor to be an instance of the ExprNodeGenericFuncDesc.
   * We exit the recursion when we find an instance of ExprNodeColumnDesc and set its column name to internal name
   * @param exprNodeDesc
   */
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
        //continue until you find an instance of the ExprNodeColumnDesc
        setFilterPredicateCol(colExpr);
      }
    }

  }


  public static NewQueryFilterSchemaProc getNewQueryFilterSchemaProc(){
    return new NewQueryFilterSchemaProc();
  }


}
