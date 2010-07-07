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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.index.HiveIndex.IndexType;
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
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * GbToCompactSumIdxRewrite.
 * This rule rewrites group by query to the query over
 * simple tablescan over COPMACT_SUMMARY index, if there is compact
 * summary index on the group by key(s) or the distinct column(s)
 * Eg.
 * <code>
 *   select key
 *   from table
 *   group by key;
 * </code>
 *  to
 *  <code>
 *   select key
 *   from table_cmpt_sum_idx;
 *  </code>
 * @see org.apache.hadoop.hive.ql.index.HiveIndex
 * @see org.apache.hadoop.hive.ql.index.HiveIndex.COMPACT_SUMMARY_TABLE
 *
 */
public class GbToCompactSumIdxRewrite extends HiveRwRule {
  //See if there is group by or distinct
  //and check if the columns there is index over the columns involved.

  private final Hive m_hiveInstance;


  public GbToCompactSumIdxRewrite(Hive hiveInstance, Log log) {
    super(log);
    m_hiveInstance = hiveInstance;
  }

  class GbToCompactSumIdxRewriteContext extends  HiveRwRuleContext {
    public Table m_indexTableMetaData;
    public String m_sClauseName;
    public boolean m_bIsDistinct;
    public String m_sOrigBaseTableAlias;
    public boolean m_bRemoveGroupBy;
  }

  class CollectColRefNames implements NodeProcessor  {
    /**
     *     Column names of column references found in root ast node
     *     passed in constructor
     */
    private final List<String> m_vColNames;
    /**
     * If true, Do not return column references which are children of functions
     * Just return column references which are direct children of passed
     * rootNode
     */
    private boolean m_bOnlyDirectChildren;
    public CollectColRefNames(ASTNode rootNode)  {
      m_vColNames = new ArrayList<String>();
      init(rootNode, false);
    }
    public CollectColRefNames(ASTNode rootNode, boolean bOnlyDirectChildren)  {
      m_vColNames = new ArrayList<String>();
      init(rootNode, bOnlyDirectChildren);
    }

    private void init(ASTNode rootNode, boolean bOnlyDirectChildren)  {
      if( rootNode != null )  {
        ArrayList<Node> alStartNode = new ArrayList<Node>();
        m_bOnlyDirectChildren = bOnlyDirectChildren;
        alStartNode.add(rootNode);
        Map<Rule, NodeProcessor> noSpecialRule = new HashMap<Rule, NodeProcessor>();
        DefaultRuleDispatcher ruleDispatcher = new DefaultRuleDispatcher(this, noSpecialRule  , null);
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(ruleDispatcher);
        try {
          graphWalker.startWalking(alStartNode, null);
        } catch (SemanticException e)  {
          getLogger().warn("Problem while traversing tree");
          //TODO: Rethrow exception ?
        }
      }
    }

    public List<String> getColRefs()  {
        return m_vColNames;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ASTNode astNode = (ASTNode) nd;
      if( astNode.getType() == HiveParser.TOK_TABLE_OR_COL )  {
        if( m_bOnlyDirectChildren == true )  {
          //If we want only direct children, It must have at 3 prev nodes in
          //stack in case of select expr and 2 in case of others (rootnode and
          //tok_table_or_col node)
          //Eg. For select-list , stack would normaly look like
          //ROOT_NODE, TOK_SELEXPR, TOK_TABLE_OR_COL i.e. 3 nodes
          if( !(( stack.size() == 3 &&
                ((ASTNode)stack.get(1)).getType() ==  HiveParser.TOK_SELEXPR ) ||
                ( stack.size() == 2 )) ) {
            return null;
          }
        }
        //COLNAME or COLNAME AS COL_ALIAS
        ASTNode internalNode = (ASTNode) astNode.getChild(0);
        m_vColNames.add(internalNode.getText());
      }

      return null;
    }
  }


  private List<Table> getIndexTable(Table baseTableMetaData, List<IndexType> vIndexType)  {
    List<String> vIndexTableName = baseTableMetaData.getIndexTableName();
    List<Table> vIndexTable = new ArrayList<Table>();
    for( int i = 0; i < vIndexTableName.size(); i++) {
      Table indexTable = null;
      try {
        indexTable =
          m_hiveInstance.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                            vIndexTableName.get(i),
                            false/*bThrowException*/);
        if( indexTable == null ) {
          getLogger().info("Index table " + vIndexTableName.get(i) + " could not be found");
          continue;
        }
        String sIndexType = MetaStoreUtils.getIndexType(indexTable.getTTable());
        boolean bIsValidIndex = false;
        for( int iIdxType = 0; iIdxType < vIndexType.size(); iIdxType++) {
          if( sIndexType.equalsIgnoreCase(vIndexType.get(iIdxType).getName()) ) {
            bIsValidIndex |= true;
          }
        }
        if( bIsValidIndex == false ) {
          continue;
        }
        vIndexTable.add(indexTable);
      } catch (HiveException e) {
      }
    }
    return vIndexTable;
  }


  private List<String> getChildColRefNames(ASTNode rootExpr, boolean bOnlyDirectChildren)  {
    return new CollectColRefNames(rootExpr, bOnlyDirectChildren).getColRefs();
  }

  private List<String> getChildColRefNames(ASTNode rootExpr)  {
    return new CollectColRefNames(rootExpr).getColRefs();
  }


  @Override
  public boolean canApplyThisRule(QB qb) {

    if( getRwFlag(HiveConf.ConfVars.HIVE_QL_RW_GB_TO_IDX) == false ) {
      return false;
    }

    //Multiple table not supported yet
    if( (qb.getTabAliases().size() != 1) ||
        (qb.getSubqAliases().size() != 0) ) {
      getLogger().debug("Query has more than one table or subqueries, " +
      		"that is not supported with rewrite " + getName());
      return false;
    }

    if( qb.getQbJoinTree() != null )  {
      getLogger().debug("Query has joins, " +
          "that is not supported with rewrite " + getName());
      return false;
    }
    //--------------------------------------------
    //Get clause information.
    QBParseInfo qbParseInfo = qb.getParseInfo();
    Set<String> clauseNameSet = qbParseInfo.getClauseNames();
    if( clauseNameSet.size() != 1 ) {
      return false;
    }
    Iterator<String> itrClauseName = clauseNameSet.iterator();
    String sClauseName = itrClauseName.next();

    //Check if we have sort-by clause, not yet supported
    if( qbParseInfo.getSortByForClause(sClauseName) != null )  {
      getLogger().debug("Query has sortby clause, " +
          "that is not supported with rewrite " + getName());
      return false;
    }
    //Check if we have distributed-by clause, not yet supported
    if( qbParseInfo.getDistributeByForClause(sClauseName) != null)  {
      getLogger().debug("Query has distributeby clause, " +
          "that is not supported with rewrite " + getName());
      return false;
    }

    //-------------------------------------------
    //Getting agg func information.
    HashMap<String, ASTNode> mapAggrNodes = qbParseInfo.getAggregationExprsForClause(sClauseName);
    List< List<String> > vvColRefAggFuncInput = new ArrayList<List<String>>();
    if( mapAggrNodes != null )  {
      Collection<ASTNode> listAggrNodes = mapAggrNodes.values();
      Iterator<ASTNode> it = listAggrNodes.iterator();
      while( it.hasNext() )  {
        ASTNode curNode = it.next();
        int iChldCnt = curNode.getChildCount();
        if( iChldCnt != 2 ) {
          continue;
        }

        ASTNode funcNameNode = (ASTNode) curNode.getChild(0);
        String sFuncName = funcNameNode.getText();
        if( sFuncName.equalsIgnoreCase("count") == false ) {
          getLogger().debug("Agg func other than count is not supported by rewrite " + getName());
          return false;
        }
        List<String> vAggFuncInp = getChildColRefNames(curNode);
        vvColRefAggFuncInput.add(vAggFuncInp);
      }
    }



    //--------------------------------------------
    //Getting where clause information
    ASTNode whereClause = qbParseInfo.getWhrForClause(sClauseName);
    /*
    if( whereClause != null )  {
      getLogger().debug("Query has where clause, " +
          "that is not supported with rewrite " + getName());
      return false;
    }
    */
    List<String> predColRefs = getChildColRefNames(whereClause);


    //--------------------------------------------
    //Getting select list column names
    ASTNode rootSelExpr = qbParseInfo.getSelForClause(sClauseName);
    boolean bIsDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
    List<String> selColRefNameList = getChildColRefNames(rootSelExpr, bIsDistinct/*bOnlyDirectChildren*/);
    if( bIsDistinct == true &&
        selColRefNameList.size() != rootSelExpr.getChildCount() )  {
      getLogger().debug("Select-list has distinct and it also has some non-col-ref expression. " +
      		"Cannot apply the rewrite " + getName());
      return false;
    }

    //Getting groupby key information
    ASTNode groupByNode = qbParseInfo.getGroupByForClause(sClauseName);

    List<String> gbKeyNameList = getChildColRefNames(groupByNode, true/*bOnlyDirectChildren*/);
    if( (groupByNode != null) &&
        (gbKeyNameList.size() != groupByNode.getChildCount()) )  {
      getLogger().debug("Group-by-key-list has some non-col-ref expression. " +
          "Cannot apply the rewrite " + getName());
      return false;
    }

    //--------------------------------------------
    //Get Index information.
    Set<String> tableAlisesSet = qb.getTabAliases();
    Iterator<String> tableAliasesItr = tableAlisesSet.iterator();
    String sTableAlias = tableAliasesItr.next();
    String sTableName = qb.getTabNameForAlias(sTableAlias);
    Table tableQlMetaData = qb.getMetaData().getTableForAlias(sTableAlias);

    if( !tableQlMetaData.hasIndex() ) {
      getLogger().debug("Table " + sTableName + " does not have indexes. Cannot apply rewrite " + getName());
      return false;
    }

    List<IndexType> vValidIndexToBeUsed = new ArrayList<IndexType>();
    vValidIndexToBeUsed.add(IndexType.COMPACT_SUMMARY_TABLE);
    vValidIndexToBeUsed.add(IndexType.SUMMARY_TABLE);
    List<Table> vIndexTable = getIndexTable(tableQlMetaData,vValidIndexToBeUsed);
    if( vIndexTable.size() == 0 ) {
      getLogger().debug("Table " + sTableName + " does not have compat summary " +
      		"index. Cannot apply rewrite " + getName());
      return false;
    }

    Table indexTable = null;
    for( int iIdxTbl = 0;iIdxTbl < vIndexTable.size(); iIdxTbl++)  {
      indexTable = vIndexTable.get(iIdxTbl);

      //If we have agg function, check if we have SUMMARY table, we currently only support that.
      if( vvColRefAggFuncInput.size() > 0 )  {
        String sIndexType = MetaStoreUtils.getIndexType(indexTable.getTTable());
        if( sIndexType.equalsIgnoreCase(HiveIndex.IndexType.SUMMARY_TABLE.getName()) == false ) {
          continue;
        }
      }
      //Getting index key columns
      List<FieldSchema> vCols = indexTable.getCols();
      Set<String> idxKeyColsNames = new TreeSet<String>();
      for( int i = 0;i < vCols.size(); i++) {
        //Skipping index metadata columns
        if ( vCols.get(i).getName().equals(HiveIndex.IDX_BUCKET_COL_NAME) ) {
          continue;
        }
        //Skipping index metadata columns
        if ( vCols.get(i).getName().equals(HiveIndex.IDX_OFFSET_COL_NAME) ) {
          continue;
        }
        idxKeyColsNames.add(vCols.get(i).getName());
      }

      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if( idxKeyColsNames.containsAll(selColRefNameList) == false ) {
        getLogger().debug("Select list has non index key column : " +
        		" Cannot use this index  " + indexTable.getTableName());
        continue;
      }

      //We need to check if all columns from index appear in select list only
      //in case of DISTINCT queries, In case group by queries, it is okay as long
      //as all columns from index appear in group-by-key list.
      if( bIsDistinct ) {
        //Check if all columns from index are part of select list too
        if( selColRefNameList.containsAll(idxKeyColsNames) == false )  {
          getLogger().debug("Index has non select list columns " +
              " Cannot use this index  " + indexTable.getTableName());
          continue;
        }
      }

      //--------------------------------------------
      //Check if all columns in where predicate are part of index key columns
      //TODO: Currently we allow all predicates , would it be more efficient (or at least not worse)
      //to read from index_table and not from baseTable ?
      if( idxKeyColsNames.containsAll(predColRefs) == false ) {
        getLogger().debug("Predicate column ref list has non index key column : " +
            " Cannot use this index  " + indexTable.getTableName());
        continue;
      }

      if( bIsDistinct == false )  {
        //--------------------------------------------
        //For group by, we need to check if all keys are from index columns
        //itself. Here gb key order can be different than index columns but that does
        //not really matter for final result.
        //Eg. select c1, c2 from src group by c2, c1;
        //we can rewrite this one to s
        //select c1, c2 from src_cmpt_sum_idx;
        if( idxKeyColsNames.containsAll(gbKeyNameList) == false ) {
            getLogger().debug("Groupby key-list has non index key column  " +
                " Cannot use this index  " + indexTable.getTableName());
              continue;
        }

        if( gbKeyNameList.containsAll(idxKeyColsNames) == false )  {
          getLogger().debug("Index has some columns which do not appear in gb key columns " +
              " Cannot use this index  " + indexTable.getTableName());
          continue;
        }

        //If we have agg function (currently only COUNT is supported), check if its input are
        //from index. we currently only support that.
        if( vvColRefAggFuncInput.size() > 0 )  {
          for( int iAggFuncIdx = 0; iAggFuncIdx < vvColRefAggFuncInput.size(); iAggFuncIdx++)  {
            if( idxKeyColsNames.containsAll(vvColRefAggFuncInput.get(iAggFuncIdx)) == false ) {
              getLogger().debug("Agg Func input is not present in index key columns. " +
              		"Currently only agg func on index columns are supported");
              continue;
            }

          }

        }

      }
      GbToCompactSumIdxRewriteContext rwContext = new GbToCompactSumIdxRewriteContext();
      rwContext.m_indexTableMetaData = indexTable;
      rwContext.m_sClauseName = sClauseName;
      rwContext.m_bIsDistinct = bIsDistinct;
      String sIndexType = MetaStoreUtils.getIndexType(indexTable.getTTable());
      rwContext.m_sOrigBaseTableAlias = sTableAlias;
      rwContext.m_bRemoveGroupBy = (sIndexType.equalsIgnoreCase(HiveIndex.IndexType.COMPACT_SUMMARY_TABLE.getName()) == true);
      setContext(rwContext);
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
    Table indexTable = rwContext.m_indexTableMetaData;
    String sIndexTableName = indexTable.getTableName();
    QBParseInfo qbParseInfo = oldQb.getParseInfo();
    String sClauseName = rwContext.m_sClauseName;
    QBMetaData qbMetaData = oldQb.getMetaData();

    //Change query sourcetable to index table.
    oldQb.replaceTableAlias(rwContext.m_sOrigBaseTableAlias,
        sIndexTableName/*aliase*/,
        sIndexTableName/*tableName*/,
        sClauseName/*clauseName*/);
    qbMetaData.setSrcForAlias(sIndexTableName, indexTable);

    if( rwContext.m_bIsDistinct ) {
      //Remove distinct
      qbParseInfo.clearDistinctFlag(sClauseName);
    }
    else  {
      if( rwContext.m_bRemoveGroupBy == true ) {
        //Remove groupby
        qbParseInfo.clearGroupBy(sClauseName);
      }
    }
    //We aint changing qb here, what we change is ind
    return oldQb;
  }

  @Override
  public boolean applyTopDown() {
    //This rewrite needs to be applied bottom up
    return false;
  }

}
