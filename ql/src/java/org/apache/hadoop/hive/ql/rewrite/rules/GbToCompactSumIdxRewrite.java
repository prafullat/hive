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
 *  XTODO: Enhance the comment by mentioning
 *  - things that are supported
 *  - things that are not supported
 *
 * @see org.apache.hadoop.hive.ql.index.HiveIndex
 * @see org.apache.hadoop.hive.ql.index.HiveIndex.CompactIndexHandler
 *
 */
public class GbToCompactSumIdxRewrite extends HiveRwRule {
  // See if there is group by or distinct
  // and check if the columns there is index over the columns involved.

  private final Hive hiveInstance;
  // XTODO: Is this thread-safe?
  private static int subqueryCounter = 0;
  private static final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";

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

    public CollectColRefNames(ASTNode rootNode)  {
      colNameList = new ArrayList<String>();
      init(rootNode, false);
    }
    public CollectColRefNames(ASTNode rootNode, boolean onlyDirectChildren)  {
      colNameList = new ArrayList<String>();
      init(rootNode, onlyDirectChildren);
    }

    private void init(ASTNode rootNode, boolean onlyDirectChildren)  {
      onlyDirectChildren = false;
      // In case of rootNode == null, return empty col ref list.
      if (rootNode != null)  {
        ArrayList<Node> startNodeList = new ArrayList<Node>();
        this.onlyDirectChildren = onlyDirectChildren;
        startNodeList.add(rootNode);
        Map<Rule, NodeProcessor> noSpecialRule = new HashMap<Rule, NodeProcessor>();
        DefaultRuleDispatcher ruleDispatcher =
          new DefaultRuleDispatcher(this, noSpecialRule, null);
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(ruleDispatcher);
        try {
          graphWalker.startWalking(startNodeList, null);
        } catch (SemanticException e)  {
          getLogger().warn("Problem while traversing tree");
          // XTODO: Re-throw exception ?
        }
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

      ASTNode astNode = (ASTNode) nd;
      if (astNode.getType() == HiveParser.TOK_TABLE_OR_COL)  {
        if (onlyDirectChildren == true)  {
          // If we want only direct children, It must have at 3 prev nodes in
          // stack in case of select expr and 2 in case of others (rootnode and
          // tok_table_or_col node)
          // E.g. For select-list , stack would normally look like
          // ROOT_NODE, TOK_SELEXPR, TOK_TABLE_OR_COL i.e. 3 nodes
          // XTODO: Refine this check.
          if (!((stack.size() == 3 &&
                ((ASTNode)stack.get(1)).getType() ==  HiveParser.TOK_SELEXPR) ||
                 (stack.size() == 2)
                )
             ) {
            return null;
          }
        }
        //COLNAME or COLNAME AS COL_ALIAS
        ASTNode internalNode = (ASTNode) astNode.getChild(0);
        colNameList.add(internalNode.getText().toLowerCase());
      }

      return null;
    }
  }


  private List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes) {
    List<Index> indexesOnTable = baseTableMetaData.getAllIndexes();
    List<Index> matchingIndexes = new ArrayList<Index>();
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

  private List<String> getChildColRefNames(ASTNode rootExpr, boolean onlyDirectChildren)  {
    return new CollectColRefNames(rootExpr, onlyDirectChildren).getColRefs();
  }

  private List<String> getChildColRefNames(ASTNode rootExpr)  {
    return new CollectColRefNames(rootExpr).getColRefs();
  }

  @Override
  public boolean canApplyThisRule(QB qb) {
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
    //Get clause information.
    QBParseInfo qbParseInfo = qb.getParseInfo();
    Set<String> clauseNameSet = qbParseInfo.getClauseNames();
    if (clauseNameSet.size() != 1) {
      return false;
    }
    Iterator<String> clauseNameIter = clauseNameSet.iterator();
    String clauseName = clauseNameIter.next();

    // Check if we have sort-by clause, not yet supported,
    // XTODO: to be supported in future.
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
        List<String> aggFuncInput = getChildColRefNames(curNode);
        colRefAggFuncInputList.add(aggFuncInput);
        aggASTNodesList.add(curNode);
      }
    }
    //--------------------------------------------
    //Get Index information.
    // XTODO: Move the index check to beginning of the method.
    Set<String> tableAlisesSet = qb.getTabAliases();
    Iterator<String> tableAliasesItr = tableAlisesSet.iterator();
    String tableAlias = tableAliasesItr.next();
    String tableName = qb.getTabNameForAlias(tableAlias);
    Table tableQlMetaData = qb.getMetaData().getTableForAlias(tableAlias);

    if (!tableQlMetaData.hasIndex()) {
      getLogger().debug("Table " + tableName + " does not have indexes. Cannot apply rewrite "
        + getName());
      return false;
    }

    List<String> idxType = new ArrayList<String>();
    idxType.add(SUPPORTED_INDEX_TYPE);
    List<Index> indexTables = getIndexes(tableQlMetaData, idxType);
    if (indexTables.size() == 0) {
      getLogger().debug("Table " + tableName + " does not have compact index. " +
        "Cannot apply rewrite " + getName());
      return false;
    }

    //--------------------------------------------
    //Getting where clause information
    ASTNode whereClause = qbParseInfo.getWhrForClause(clauseName);
    List<String> predColRefs = getChildColRefNames(whereClause);

    //--------------------------------------------
    //Getting select list column names
    ASTNode rootSelExpr = qbParseInfo.getSelForClause(clauseName);
    boolean isDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
    List<String> selColRefNameList = getChildColRefNames(rootSelExpr,
      isDistinct //onlyDirectChildren
      );
    if (isDistinct == true &&
        selColRefNameList.size() != rootSelExpr.getChildCount())  {
      getLogger().debug("Select-list has distinct and it also has some non-col-ref expression. " +
        "Cannot apply the rewrite " + getName());
      return false;
    }

    //Getting GroupBy key information
    ASTNode groupByNode = qbParseInfo.getGroupByForClause(clauseName);
    List<String> gbKeyNameList = getChildColRefNames(groupByNode,
                                                     true //onlyDirectChildren
                                                    );
    List<String> gbKeyAllColRefList = getChildColRefNames(groupByNode);


    if (colRefAggFuncInputList.size() > 0 && groupByNode == null)  {
      getLogger().debug("Currently count function needs group by on key columns, "
        + "Cannot apply this rewrite");
      return false;
    }

    Index indexTable = null;
    for (int iIdxTbl = 0;iIdxTbl < indexTables.size(); iIdxTbl++)  {
      boolean removeGroupBy = true;
      boolean optimizeCount = false;

      indexTable = indexTables.get(iIdxTbl);

      //Getting index key columns
      List<Order> idxColList = indexTable.getSd().getSortCols();

      Set<String> idxKeyColsNames = new TreeSet<String>();
      for (int i = 0;i < idxColList.size(); i++) {
        idxKeyColsNames.add(idxColList.get(i).getCol().toLowerCase());
      }


      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if (idxKeyColsNames.containsAll(selColRefNameList) == false) {
        getLogger().debug("Select list has non index key column : " +
          " Cannot use this index  " + indexTable.getIndexName());
        continue;
      }

      // We need to check if all columns from index appear in select list only
      // in case of DISTINCT queries, In case group by queries, it is okay as long
      // as all columns from index appear in group-by-key list.
      if (isDistinct) {
        // Check if all columns from index are part of select list too
        if (selColRefNameList.containsAll(idxKeyColsNames) == false)  {
          getLogger().debug("Index has non select list columns " +
            " Cannot use this index  " + indexTable.getIndexName());
          continue;
        }
      }

      //--------------------------------------------
      // Check if all columns in where predicate are part of index key columns
      // XTODO: Currently we allow all predicates , would it be more efficient
      // (or at least not worse) to read from index_table and not from baseTable?
      if (idxKeyColsNames.containsAll(predColRefs) == false) {
        getLogger().debug("Predicate column ref list has non index key column : " +
          " Cannot use this index  " + indexTable.getIndexName());
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

        // XTODO: See if this can be relaxed.
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
          hiveInstance.getTable(indexTable.getDbName(),
            indexTable.getIndexTableName());
      } catch (HiveException e) {
        // XTODO: Log error?
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
        // XTODO: If the index table column name changes, this will need to be updated.
        rwContext.inputToSizeFunc ="`_offsets`";
        if (!removeGroupBy)  {
          rwContext.optimizeCountWithCmplxGbKey = true;
          rwContext.removeGroupBy = false;
        }
        else {
          rwContext.optimizeCountWithSimpleGbKey = true;
        }
      }
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
          // XTODO: assert for assumption (astNode.getChildCount() == 1)?
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
      // Add new QueryBlock - SubQuery over idx table
      // Remove meta data and table from this
      QBExpr subqueryBlockExpr = new QBExpr("subquery_alias_"+ subqueryCounter);
      // This is subquery
      subqueryBlockExpr.setOpcode(QBExpr.Opcode.NULLOP);

      QB subqueryBlock = new QB("gdto_idx_" + subqueryCounter, null, true);
      subqueryBlockExpr.setQB(subqueryBlock);
      subqueryBlock.setTabAlias(indexTableName, indexTableName);
      subqueryBlock.getMetaData().setSrcForAlias(indexTableName, indexTable);
      // Adding temp insert clause
      ASTNode dirNode = new ASTNode(new CommonToken(HiveParser.TOK_DIR, "TOK_DIR"));
      ASTNode tmpFileNode = new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE"));
      dirNode.addChild(tmpFileNode);
      subqueryBlock.getParseInfo().setDestForClause(clauseName, dirNode);

      ASTNode selNode = new ASTNode(new CommonToken(HiveParser.TOK_SELECT, "TOK_SELECT"));

      for (int idxCnt = 0; idxCnt < rwContext.idxKeyList.size(); idxCnt++)  {
        List<String> keyNameList = new ArrayList<String>();
        keyNameList.add(rwContext.idxKeyList.get(0));
        ASTNode selExprNode = subqueryBlock.newSelectListExpr(false, null, keyNameList);
        selNode.addChild(selExprNode);
      }
      List<String> sumInputList = new ArrayList<String>();
      sumInputList.add(rwContext.inputToSizeFunc);
      ASTNode sumNode = subqueryBlock.newSelectListExpr(true, "size", sumInputList);
      // XOTO: Need to ensure the new identifier `_ofsset_count` is unique (not used in
      // the query block already).
      sumNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,"`_offset_count`")));
      selNode.addChild(sumNode);
      if (rwContext.countFuncAstNode != null)  {
        ASTNode astNode = rwContext.countFuncAstNode;
        astNode.setChild(0, new ASTNode(new CommonToken(HiveParser.Identifier,"sum")));
        ASTNode colRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL,
          "TOK_TABLE_OR_COL"));
        colRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
          "`_offset_count`")));
        astNode.setChild(1, colRefNode);
      }
      subqueryBlock.getParseInfo().setAggregationExprsForClause(clauseName,
        new LinkedHashMap<String, ASTNode>());
      subqueryBlock.getParseInfo().setDistinctFuncExprForClause(clauseName, null);
      subqueryBlock.getParseInfo().setSelExprForClause(clauseName, selNode);
      oldQb.removeTable(rwContext.origBaseTableAlias);
      oldQb.setSubqAlias("idx_table_" + subqueryCounter, subqueryBlockExpr);
      subqueryCounter++;
    }

    // We aren't changing GB here, what we change is internal information.
    return oldQb;
  }

  @Override
  public boolean applyTopDown() {
    // This rewrite needs to be applied bottom up
    return false;
  }
}
