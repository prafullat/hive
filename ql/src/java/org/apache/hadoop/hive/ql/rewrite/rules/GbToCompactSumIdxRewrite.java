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

package org.apache.hadoop.hive.ql.rewrite.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.antlr.runtime.CommonToken;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implements GroupBy clause rewrite using compact index.
 * This rule rewrites GroupBy query over base table to the query over simple table-scan over
 * index table, if there is index on the group by key(s) or the distinct column(s).
 * E.g.
 * <code>
 *   select key
 *   from table
 *   group by key;
 * </code>
 *  to
 *  <code>
 *   select key
 *   from idx_table;
 *  </code>
 *
 *  The rewrite supports following queries
 *  - Queries having only those col refs that are in the index key.
 *  - Queries that have index key col refs
 *    - in SELECT
 *    - in WHERE
 *    - in GROUP BY
 *  - Queries with agg func COUNT(literal) or COUNT(index key col ref)
 *    in SELECT
 *  - Queries with SELECT DISTINCT index key col refs
 *  - Queries having a subquery satisfying above condition (only the
 *    subquery is rewritten)
 *
 *  FUTURE:
 *  - Many of the checks for above criteria rely on equivalence of
 *    expressions, but such framework/mechanism of expression equivalence
 *    isn't present currently or developed yet. This needs to be supported
 *    in order for better robust checks. This is critically important for
 *    correctness of a query rewrite system.
 *    - Also this code currently directly works on the parse tree data
 *      structs (AST nodes) for checking, manipulating query data structure.
 *      If such expr equiv mechanism is to be developed, it would be important
 *      to think and reflect on whether to continue use the parse tree
 *      data structs (and enhance those classes with such equivalence methods)
 *      or to create independent hierarchies of data structs and classes
 *      for the exprs and develop that equivalence mechanism on that new
 *      class hierarchy, code.
 *
 * @see org.apache.hadoop.hive.ql.index.HiveIndex
 * @see org.apache.hadoop.hive.ql.index.HiveIndex.CompactIndexHandler
 *
 */
public class GbToCompactSumIdxRewrite extends HiveRwRule {
  // See if there is group by or distinct
  // and check if the columns there is index over the columns involved.

  private final Hive hiveInstance;
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
  private static final String COMPACT_IDX_BUCKET_COL = "_bucketname";
  private static final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";
  private static ThreadLocal<Integer> subqueryCounter =
    new ThreadLocal<Integer> () {
      @Override
      protected Integer initialValue() {
        return new Integer(0);
    }
  };


  private void incrementSubqueryCounter(int incr) {
    subqueryCounter.set(subqueryCounter.get() + incr);
    // Check for roll-over after increment.
    assert(subqueryCounter.get() >= 0);
  }

  public GbToCompactSumIdxRewrite(Hive hiveInstance, Log log) {
    super(log);
    this.hiveInstance = hiveInstance;
  }

  class GbToCompactSumIdxRewriteContext extends  HiveRwRuleContext {
    public GbToCompactSumIdxRewriteContext()  {
      indexTableMetadata = null;
      clauseName = null;
      isDistinct = false;
      origBaseTableAlias = null;
      removeGroupBy = false;
      countFuncAstNode = null;
      optimizeCountWithCmplxGbKey = false;
      optimizeCountWithSimpleGbKey = false;
      inputToSizeFunc = null;
      fromNode = null;
      tabNode = null;
    }
    private Table indexTableMetadata;
    private String clauseName;
    private boolean isDistinct;
    private String origBaseTableAlias;
    private boolean removeGroupBy;
    private ASTNode countFuncAstNode;
    private List<String> idxKeyList;
    private boolean optimizeCountWithCmplxGbKey;
    private boolean optimizeCountWithSimpleGbKey;
    private String inputToSizeFunc;
    private ASTNode fromNode;
    private ASTNode tabNode;
  }

  class CollectColRefNames implements NodeProcessor  {
    /**
     * Column names of column references found in root AST node
     * passed in constructor.
     */
    private final List<String> colNameList;
    /**
     * If true, Do not return column references which are children of functions
     * Just return column references which are direct children of passed
     * rootNode.
     */
    private boolean onlyDirectChildren;
    private ASTNode rootNode;

    public CollectColRefNames(ASTNode rootNode) throws SemanticException {
      colNameList = new ArrayList<String>();
      init(rootNode, false);
    }
    public CollectColRefNames(ASTNode rootNode, boolean onlyDirectChildren)
      throws SemanticException {
      colNameList = new ArrayList<String>();
      init(rootNode, onlyDirectChildren);
    }

    private void init(ASTNode rootNode, boolean onlyDirectChildren) throws SemanticException {
      this.onlyDirectChildren = onlyDirectChildren;
      this.rootNode = rootNode;
      // In case of rootNode == null, return empty col ref list.
      if (rootNode != null)  {
        ArrayList<Node> startNodeList = new ArrayList<Node>();
        startNodeList.add(rootNode);
        Map<Rule, NodeProcessor> noSpecialRule = new HashMap<Rule, NodeProcessor>();
        DefaultRuleDispatcher ruleDispatcher =
          new DefaultRuleDispatcher(this, noSpecialRule, null);
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(ruleDispatcher);
        graphWalker.startWalking(startNodeList, null);
      }
    }

    public List<String> getColRefs()  {
        return colNameList;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // The traversal is depth-first search. The stack param here holds the visited node trail
      // in the traversal.

      // We are interested in child of curNode being visited
      // in the subtree under rootNode.
       ASTNode curNode = (ASTNode) nd;
       boolean captureCurNodeChild = true;

       assert(stack.size() == 0
              || (stack.size() > 0 && rootNode == stack.get(0))
             );
       if (curNode.getType() == HiveParser.TOK_TABLE_OR_COL)  {

         // For onlyDirectChildren, currently we support only following cases
         // (i.e. we only try to look for TABLE_OR_COL nodes just below or just very
         // near below the rootNode):
         // case 1:
         //    rootNode curNode
         //       0        1                 # <- stack elements (stack size is 2)
         // case 2:
         //    rootNode  selExprNode curNode
         //       0        1           2     # <- stack elements (stack size is 3)
         if (onlyDirectChildren == true)  {
           if ( stack.size() == 2
                || (stack.size() == 3 && ((ASTNode) stack.get(1)).getType() == HiveParser.TOK_SELEXPR)
              ) {
             captureCurNodeChild = true;
           } else {
             captureCurNodeChild = false;
           }
         }
         if (captureCurNodeChild) {
           //add curNode's child to list of cols
           //COLNAME or COLNAME AS COL_ALIAS
           ASTNode internalNode = (ASTNode) curNode.getChild(0);
           colNameList.add(internalNode.getText().toLowerCase());
         }
       }
      return null;
    }
  }

  class LocateFromTabNodeParent implements NodeProcessor  {
    private final ASTNode tableRefNodeToLocate;
    private ASTNode parentTokFrom;
    private ASTNode parentTokTable;

    public LocateFromTabNodeParent(ASTNode rootNode, ASTNode tableRefNode)
      throws SemanticException {
      assert(tableRefNode.getType() == HiveParser.Identifier);
      tableRefNodeToLocate = tableRefNode;
      parentTokFrom = null;
      parentTokTable = null;
      locate(rootNode);
    }

    public void locate(ASTNode rootNode) throws SemanticException {
      ArrayList<Node> startNodeList = new ArrayList<Node>();
      startNodeList.add(rootNode);
      Map<Rule, NodeProcessor> noSpecialRule = new HashMap<Rule, NodeProcessor>();
      DefaultRuleDispatcher ruleDispatcher =
        new DefaultRuleDispatcher(this, noSpecialRule, null);
      DefaultGraphWalker graphWalker = new DefaultGraphWalker(ruleDispatcher);
      graphWalker.startWalking(startNodeList, null);
    }

    ASTNode getParentTokFromNode() {
      return parentTokFrom;
    }

    ASTNode getParentTokTableNode() {
      return parentTokTable;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

       ASTNode curNode = (ASTNode) nd;
       if (curNode.getType() == HiveParser.Identifier
           && curNode == tableRefNodeToLocate) {
           assert(stack.size() >= 4);
           // Stack should look like:
           // TOK_QUERY -> TOK_FROM -> TOK_TABREF -> TOK_TABLE_OR_COL
           //                                            (curNode)
           assert(((ASTNode)stack.get(stack.size() - 1)).getType() == HiveParser.Identifier
                  && stack.get(stack.size() - 1) == curNode);
           assert(((ASTNode)stack.get(stack.size() - 2)).getType() == HiveParser.TOK_TABREF);
           assert(((ASTNode)stack.get(stack.size() - 3)).getType() == HiveParser.TOK_FROM);
           assert(((ASTNode)stack.get(stack.size() - 4)).getType() == HiveParser.TOK_QUERY);
           parentTokFrom = (ASTNode) (stack.get(stack.size() - 3));
           parentTokTable = (ASTNode) (stack.get(stack.size() - 2));
       }
      return null;
    }
  }

  private List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes) {
    List<Index> matchingIndexes = new ArrayList<Index>();
    List<Index> indexesOnTable = null;

    try {
      short maxNumOfIndexes = 1024; // XTODO: Hardcoding. Need to know if
                                    // there's a limit (and what is it) on
                                    // # of indexes that can be created
                                    // on a table. If not, why is this param
                                    // required by metastore APIs?
      indexesOnTable = baseTableMetaData.getAllIndexes(maxNumOfIndexes);
    } catch (HiveException e) {
      return matchingIndexes; // Return empty list (trouble doing rewrite
                              // shouldn't stop regular query execution,
                              // if there's serious problem with metadata
                              // or anything else, it's assumed to be
                              // checked & handled in core hive code itself.
    }

    for (int i = 0; i < indexesOnTable.size(); i++) {
      Index index = null;
      index = indexesOnTable.get(i);
      // The handler class implies the type of the index (e.g. compact
      // summary index would be:
      // "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler").
      String indexType = index.getIndexHandlerClass();
      for (int  j = 0; j < matchIndexTypes.size(); j++) {
        if (indexType.equals(matchIndexTypes.get(j))) {
          matchingIndexes.add(index);
          break;
        }
      }
    }
    return matchingIndexes;
  }

  private List<String> getChildColRefNames(ASTNode rootExpr, boolean onlyDirectChildren)
    throws SemanticException {
    return new CollectColRefNames(rootExpr, onlyDirectChildren).getColRefs();
  }

  private List<String> getChildColRefNames(ASTNode rootExpr) throws SemanticException {
    return new CollectColRefNames(rootExpr).getColRefs();
  }

  @Override
  public boolean canApplyThisRule(QB qb, ASTNode rootNode) {
    if (getRwFlag(HiveConf.ConfVars.HIVE_QL_RW_GB_TO_IDX) == false) {
      getLogger().debug("Conf variable " + HiveConf.ConfVars.HIVE_QL_RW_GB_TO_IDX.name()
        + " is set to false, not doing rewrite " + getName());
      return false;
    }

    //Multiple table not supported yet
    if ((qb.getTabAliases().size() != 1) ||
        (qb.getSubqAliases().size() != 0)) {
      getLogger().debug("Query has more than one table or subqueries, " +
        "that is not supported with rewrite " + getName());
      return false;
    }

    if (qb.getQbJoinTree() != null)  {
      getLogger().debug("Query has joins, " +
        "that is not supported with rewrite " + getName());
      return false;
    }


    //--------------------------------------------
    // Get Index information.
    Set<String> tableAlisesSet = qb.getTabAliases();
    Iterator<String> tableAliasesItr = tableAlisesSet.iterator();
    String tableAlias = tableAliasesItr.next();
    String tableName = qb.getTabNameForAlias(tableAlias);
    Table tableQlMetaData = qb.getMetaData().getTableForAlias(tableAlias);

    //--------------------------------------------
    // Get the TOK_QUERY for the current TOK_FROM
    ASTNode tableNode = qb.getTableNodeForAlias(tableAlias);
    ASTNode fromNode = null;
    ASTNode tabNode = null;
    try {
      LocateFromTabNodeParent l = new LocateFromTabNodeParent(rootNode, tableNode);
      fromNode = l.getParentTokFromNode();
      tabNode = l.getParentTokTableNode();
    } catch (SemanticException e) {
      getLogger().debug("Got exception while trying to locate TOK_QUERY node for (TOK_FROM "
          + tableAlias + ")");
      return false;
    }
    assert(fromNode != null);
    assert(tabNode != null);


    List<String> idxType = new ArrayList<String>();
    idxType.add(SUPPORTED_INDEX_TYPE);
    List<Index> indexTables = getIndexes(tableQlMetaData, idxType);
    if (indexTables.size() == 0) {
      getLogger().debug("Table " + tableName + " does not have compact index. " +
        "Cannot apply rewrite " + getName());
      return false;
    }

    //--------------------------------------------
    //Get clause information.
    QBParseInfo qbParseInfo = qb.getParseInfo();
    Set<String> clauseNameSet = qbParseInfo.getClauseNames();
    if (clauseNameSet.size() != 1) {
      return false;
    }
    Iterator<String> clauseNameIter = clauseNameSet.iterator();
    String clauseName = clauseNameIter.next();

    // Check if we have sort-by clause, not yet supported,
    // TODO: to be supported in future.
    if (qbParseInfo.getSortByForClause(clauseName) != null)  {
      getLogger().debug("Query has sortby clause, " +
        "that is not supported with rewrite " + getName());
      return false;
    }
    // Check if we have distributed-by clause, not yet supported
    if (qbParseInfo.getDistributeByForClause(clauseName) != null)  {
      getLogger().debug("Query has distributeby clause, " +
        "that is not supported with rewrite " + getName());
      return false;
    }

    //-------------------------------------------
    //Getting agg func information.
    HashMap<String, ASTNode> mapAggrNodes = qbParseInfo.getAggregationExprsForClause(clauseName);
    List<List<String>> colRefAggFuncInputList = new ArrayList<List<String>>();
    List<ASTNode> aggASTNodesList = new ArrayList<ASTNode>();
    if (mapAggrNodes != null)  {
      getLogger().debug("Found " + mapAggrNodes.size() + " aggregate functions");
      if (mapAggrNodes.size() > 1)  {
        getLogger().debug("More than 1 agg funcs: Not supported by rewrite " + getName());
        return false;
      }
      Collection<ASTNode> listAggrNodes = mapAggrNodes.values();
      Iterator<ASTNode> it = listAggrNodes.iterator();
      while (it.hasNext())  {
        ASTNode curNode = it.next();
        int childCount = curNode.getChildCount();
        // Check that the agg func node has 2 children (count & it's input) before the child
        // array is accessed.
        if (childCount != 2) {
          continue;
        }
        ASTNode funcNameNode = (ASTNode) curNode.getChild(0);
        String funcName = funcNameNode.getText();
        if (funcName.toLowerCase().equals("count") == false) {
          getLogger().debug("Agg func other than count is not supported by rewrite " + getName());
          return false;
        }
        try {
          List<String> aggFuncInput = getChildColRefNames(curNode);
          colRefAggFuncInputList.add(aggFuncInput);
          aggASTNodesList.add(curNode);
        } catch (SemanticException se) {
          getLogger().debug("Got exception while locating child col refs of agg func, skipping rewrite");
          return false;
        }
      }
    }

    //--------------------------------------------
    //Getting where clause information
    ASTNode whereClause = qbParseInfo.getWhrForClause(clauseName);
    List<String> predColRefs = null;
    try {
      predColRefs = getChildColRefNames(whereClause);
    } catch (SemanticException se) {
      getLogger().debug("Got exception while locating child col refs for where clause, "
        + "skipping rewrite");
      return false;
    }

    //--------------------------------------------
    //Getting select list column names
    ASTNode rootSelExpr = qbParseInfo.getSelForClause(clauseName);
    boolean isDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
    List<String> selColRefNameList = null;
    try {
      selColRefNameList = getChildColRefNames(rootSelExpr,
        isDistinct //onlyDirectChildren
        );
    } catch (SemanticException se) {
      getLogger().debug("Got exception while locating child col refs for select list, "
        + "skipping rewrite");
      return false;
    }
    if (isDistinct == true &&
        selColRefNameList.size() != rootSelExpr.getChildCount())  {
      getLogger().debug("Select-list has distinct and it also has some non-col-ref expression. " +
        "Cannot apply the rewrite " + getName());
      return false;
    }

    //Getting GroupBy key information
    ASTNode groupByNode = qbParseInfo.getGroupByForClause(clauseName);
    List<String> gbKeyNameList = null;
    List<String> gbKeyAllColRefList = null;
    try {
      gbKeyNameList = getChildColRefNames(groupByNode,
        true //onlyDirectChildren
        );
      gbKeyAllColRefList = getChildColRefNames(groupByNode);
    } catch (SemanticException se) {
      getLogger().debug("Got exception while locating child col refs for GroupBy key, "
          + "skipping rewrite");
        return false;
    }

    if (colRefAggFuncInputList.size() > 0 && groupByNode == null)  {
      getLogger().debug("Currently count function needs group by on key columns, "
        + "Cannot apply this rewrite");
      return false;
    }

    Index idx = null;
    // This code block iterates over indexes on the table and picks up the
    // first index that satisfies the rewrite criteria.
    for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      boolean removeGroupBy = true;
      boolean optimizeCount = false;

      idx = indexTables.get(idxCtr);

      //Getting index key columns
      List<Order> idxColList = idx.getSd().getSortCols();

      Set<String> idxKeyColsNames = new TreeSet<String>();
      for (int i = 0;i < idxColList.size(); i++) {
        idxKeyColsNames.add(idxColList.get(i).getCol().toLowerCase());
      }

      // Check that the index schema is as expected. This code block should
      // catch problems of this rewrite breaking when the CompactIndexHandler
      // index is changed.
      // This dependency could be better handled by doing init-time check for
      // compatibility instead of this overhead for every rewrite invocation.
      ArrayList<String> idxTblColNames = new ArrayList<String>();
      try {
        Table idxTbl = hiveInstance.getTable(idx.getDbName(),
          idx.getIndexTableName());
        for (FieldSchema idxTblCol : idxTbl.getCols()) {
          idxTblColNames.add(idxTblCol.getName());
        }
      } catch (HiveException e) {
        getLogger().debug("Got exception while locating index table, skipping rewrite");
        return false;
      }
      assert(idxTblColNames.contains(COMPACT_IDX_BUCKET_COL));
      assert(idxTblColNames.contains(COMPACT_IDX_OFFSETS_ARRAY_COL));
      assert(idxTblColNames.size() == idxKeyColsNames.size() + 2);

      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if (idxKeyColsNames.containsAll(selColRefNameList) == false) {
        getLogger().debug("Select list has non index key column : " +
          " Cannot use this index  " + idx.getIndexName());
        continue;
      }

      // We need to check if all columns from index appear in select list only
      // in case of DISTINCT queries, In case group by queries, it is okay as long
      // as all columns from index appear in group-by-key list.
      if (isDistinct) {
        // Check if all columns from index are part of select list too
        if (selColRefNameList.containsAll(idxKeyColsNames) == false)  {
          getLogger().debug("Index has non select list columns " +
            " Cannot use this index  " + idx.getIndexName());
          continue;
        }
      }

      //--------------------------------------------
      // Check if all columns in where predicate are part of index key columns
      // TODO: Currently we allow all predicates , would it be more efficient
      // (or at least not worse) to read from index_table and not from baseTable?
      if (idxKeyColsNames.containsAll(predColRefs) == false) {
        getLogger().debug("Predicate column ref list has non index key column : " +
          " Cannot use this index  " + idx.getIndexName());
        continue;
      }

      if (isDistinct == false)  {
        //--------------------------------------------
        // For group by, we need to check if all keys are from index columns
        // itself. Here GB key order can be different than index columns but that does
        // not really matter for final result.
        // E.g. select c1, c2 from src group by c2, c1;
        // we can rewrite this one to:
        // select c1, c2 from src_cmpt_idx;
        if (idxKeyColsNames.containsAll(gbKeyNameList) == false) {
          getLogger().debug("Group by key has some non-indexed columns, Cannot apply rewrite "
            + getName());
          return false;
        }

        if (idxKeyColsNames.containsAll(gbKeyAllColRefList) == false)  {
          getLogger().debug("Group by key has some non-indexed columns, Cannot apply rewrite "
            + getName());
          return false;
        }

        if (gbKeyNameList.containsAll(idxKeyColsNames) == false)  {
          // GB key and idx key are not same, don't remove GroupBy, but still do index scan
          removeGroupBy = false;
        }

        // This check prevents to remove GroupBy for cases where the GROUP BY key cols are
        // not simple expressions i.e. simple index key cols (in any order), but some
        // expressions on the the key cols.
        // e.g.
        // 1. GROUP BY key, f(key)
        //     FUTURE: If f(key) output is functionally dependent on key, then we should support
        //            it. However we don't have mechanism/info about f() yet to decide that.
        // 2. GROUP BY idxKey, 1
        //     FUTURE: GB Key has literals along with idxKeyCols. Develop a rewrite to eliminate the
        //            literals from GB key.
        // 3. GROUP BY idxKey, idxKey
        //     FUTURE: GB Key has dup idxKeyCols. Develop a rewrite to eliminate the dup key cols
        //            from GB key.
        if (gbKeyNameList.size() != groupByNode.getChildCount()) {
          getLogger().debug("Group by key can have only simple index columns, GroupBy will be"
            + " preserved by rewrite " + getName());
          removeGroupBy = false;
        }

        // FUTURE: See if this can be relaxed.
        // If we have agg function (currently only COUNT is supported), check if its input are
        // from index. we currently support only that.
        if (colRefAggFuncInputList.size() > 0)  {
          for (int aggFuncIdx = 0; aggFuncIdx < colRefAggFuncInputList.size(); aggFuncIdx++)  {
            if (idxKeyColsNames.containsAll(colRefAggFuncInputList.get(aggFuncIdx)) == false) {
              getLogger().debug("Agg Func input is not present in index key columns. Currently " +
                "only agg func on index columns are supported by rewrite" + getName());
              continue;
            }

            // If we have count on some key, check if key is same as index key,
            if (colRefAggFuncInputList.get(aggFuncIdx).size() > 0)  {
              if (colRefAggFuncInputList.get(aggFuncIdx).containsAll(idxKeyColsNames))  {
                optimizeCount = true;
              }
            }
            else  {
              optimizeCount = true;
            }
          }
        }
      }

      GbToCompactSumIdxRewriteContext rwContext = new GbToCompactSumIdxRewriteContext();
      try {
        rwContext.indexTableMetadata =
          hiveInstance.getTable(idx.getDbName(),
            idx.getIndexTableName());
      } catch (HiveException e) {
        getLogger().debug("Got exception while locating index table, skipping rewrite");
        return false;
      }
      rwContext.clauseName = clauseName;
      rwContext.idxKeyList = new ArrayList<String>();
      rwContext.idxKeyList.addAll(idxKeyColsNames);
      rwContext.isDistinct = isDistinct;
      rwContext.origBaseTableAlias = tableAlias;
      rwContext.removeGroupBy = removeGroupBy;
      if (optimizeCount)  {
        rwContext.countFuncAstNode = aggASTNodesList.get(0);
        rwContext.inputToSizeFunc = COMPACT_IDX_OFFSETS_ARRAY_COL;
        if (!removeGroupBy)  {
          rwContext.optimizeCountWithCmplxGbKey = true;
          rwContext.removeGroupBy = false;
        }
        else {
          rwContext.optimizeCountWithSimpleGbKey = true;
        }
      }
      rwContext.fromNode = fromNode;
      rwContext.tabNode = tabNode;
      setContext(rwContext);
      getLogger().debug("Now rewriting query block id " + qb.getId() +"with " + getName()
        + " rewrite");
      return true;
    }
    return false;
  }

  @Override
  public String getName() {
    return "GbToCompactSumIdxRewrite";
  }

  @Override
  public QB rewriteQb(QB oldQb) {
    GbToCompactSumIdxRewriteContext rwContext = (GbToCompactSumIdxRewriteContext)getContext();
    Table indexTable = rwContext.indexTableMetadata;
    String indexTableName = indexTable.getTableName();
    QBParseInfo qbParseInfo = oldQb.getParseInfo();

    String clauseName = rwContext.clauseName;
    QBMetaData qbMetaData = oldQb.getMetaData();
    // In case of complex GB key, we put sub-query inside this query which is on index table
    // So this anyways will be removed.
    if (rwContext.optimizeCountWithCmplxGbKey == false)  {
      // Change query source table to index table.
      oldQb.replaceTableAlias(rwContext.origBaseTableAlias
        ,indexTableName //alias
        ,indexTableName //tableName
        ,clauseName // clauseName
        );
      qbMetaData.setSrcForAlias(indexTableName, indexTable);
    }

    if (rwContext.isDistinct) {
      // Remove distinct
      qbParseInfo.clearDistinctFlag(clauseName);
    }
    else  {
      if (rwContext.removeGroupBy == true) {
        // Remove GroupBy
        qbParseInfo.clearGroupBy(clauseName);
      }
    }

    if (rwContext.optimizeCountWithSimpleGbKey == true) {
      // Replace count() with size on offset column
      if (rwContext.countFuncAstNode != null)  {
        ASTNode astNode = rwContext.countFuncAstNode;
        astNode.setChild(0, new ASTNode(new CommonToken(HiveParser.Identifier,"size")));
        ASTNode colRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL,
          "TOK_TABLE_OR_COL"));
        colRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
          rwContext.inputToSizeFunc)));
        if (astNode.getChildCount() == 2)  {
          astNode.setChild(1, colRefNode);
        }
        else {
          astNode.addChild(colRefNode);
        }
        if (rwContext.removeGroupBy == true) {
          // Make the agg func expr list empty
          qbParseInfo.setAggregationExprsForClause(clauseName,
            new LinkedHashMap<String, ASTNode>());
        }
      }
    }

    if (rwContext.optimizeCountWithCmplxGbKey == true)  {
      // Add new QueryBlock - SubQuery over idx table. Remove meta data and table from this.
      // Assumption: This generated QBExpr name is not already being used in the query.
      QBExpr subqueryBlockExpr = new QBExpr("_rw_gen_subquery_alias_" + subqueryCounter.get());
      // This is subquery
      subqueryBlockExpr.setOpcode(QBExpr.Opcode.NULLOP);

      // Assumption: This generated QB name is not already being used in the query.
      QB subqueryBlock = new QB("_rw_gen_gb_to_idx_" + subqueryCounter.get(), null, true);
      subqueryBlockExpr.setQB(subqueryBlock);
      subqueryBlock.setTabAlias(indexTableName, indexTableName);
      subqueryBlock.getMetaData().setSrcForAlias(indexTableName, indexTable);

      ASTNode tabRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABREF, "TOK_TABREF"));
      tabRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, indexTableName)));
      ASTNode fromNode = new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM"));
      fromNode.addChild(tabRefNode);

      // Adding temp insert clause
      ASTNode dirNode = new ASTNode(new CommonToken(HiveParser.TOK_DIR, "TOK_DIR"));
      ASTNode tmpFileNode = new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE"));
      dirNode.addChild(tmpFileNode);
      subqueryBlock.getParseInfo().setDestForClause(clauseName, dirNode);
      ASTNode destNode = new ASTNode(new CommonToken(HiveParser.TOK_DESTINATION, "TOK_DESTINATION"));
      destNode.addChild(dirNode);

      ASTNode selNode = new ASTNode(new CommonToken(HiveParser.TOK_SELECT, "TOK_SELECT"));

      for (int idxCnt = 0; idxCnt < rwContext.idxKeyList.size(); idxCnt++)  {
        List<String> keyNameList = new ArrayList<String>();
        keyNameList.add(rwContext.idxKeyList.get(idxCnt));
        ASTNode selExprNode = subqueryBlock.newSelectListExpr(false, null, keyNameList);
        selNode.addChild(selExprNode);
      }
      List<String> sumInputList = new ArrayList<String>();
      sumInputList.add(rwContext.inputToSizeFunc);
      ASTNode sumNode = subqueryBlock.newSelectListExpr(true, "size", sumInputList);
      // Assumption: This generated identifier name is not already being used in the query.
      sumNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,"_rw_gen_offset_count")));
      selNode.addChild(sumNode);
      if (rwContext.countFuncAstNode != null)  {
        ASTNode astNode = rwContext.countFuncAstNode;
        astNode.setChild(0, new ASTNode(new CommonToken(HiveParser.Identifier,"sum")));
        ASTNode colRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL,
          "TOK_TABLE_OR_COL"));
        colRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
          "_rw_gen_offset_count")));
        astNode.setChild(1, colRefNode);
      }

      ASTNode insNode = new ASTNode(new CommonToken(HiveParser.TOK_INSERT, "TOK_INSERT"));
      insNode.addChild(destNode);
      insNode.addChild(selNode);

      ASTNode queryNode = new ASTNode(new CommonToken(HiveParser.TOK_QUERY, "TOK_QUERY"));
      queryNode.addChild(fromNode);
      queryNode.addChild(insNode);
      ASTNode subqueryNode = new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY"));
      subqueryNode.addChild(queryNode);
      replaceChildOfNode(rwContext.fromNode, rwContext.tabNode, subqueryNode);

      subqueryBlock.getParseInfo().setAggregationExprsForClause(clauseName,
        new LinkedHashMap<String, ASTNode>());
      subqueryBlock.getParseInfo().setDistinctFuncExprForClause(clauseName, null);
      subqueryBlock.getParseInfo().setSelExprForClause(clauseName, selNode);
      oldQb.removeTable(rwContext.origBaseTableAlias);
      oldQb.setSubqAlias("_rw_gen_idx_table_" + subqueryCounter.get(), subqueryBlockExpr);
      incrementSubqueryCounter(1);
    }

    // We aren't changing GB here, what we change is internal information.
    return oldQb;
  }

  private void replaceChildOfNode(ASTNode parent, ASTNode childToReplace, ASTNode newChild) {
    for (int i = 0; i < parent.getChildCount(); i++) {
      if (parent.getChild(i) == childToReplace) {
        parent.setChild(i, newChild);
        return;
      }
    }
  }

  @Override
  public boolean applyTopDown() {
    // This rewrite needs to be applied bottom up
    return false;
  }
}
