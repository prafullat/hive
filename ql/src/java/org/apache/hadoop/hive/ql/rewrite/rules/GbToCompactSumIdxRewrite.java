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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.index.HiveIndex.IndexType;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;

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
    public String m_sOrigBaseTableAliase;

  }

  private Table getIndexTable(String sBaseTableName)  {
    List<String> indexSuffixList = new ArrayList<String>();
    Table indexTable = null;
    indexSuffixList.add("_proj_idx");
    indexSuffixList.add("_sum_idx");
    indexSuffixList.add("_cmpt_sum_idx");
    for(int i = 0; i < indexSuffixList.size(); i++)  {
      String sIndexTableName = sBaseTableName + indexSuffixList.get(i);
      try {
        indexTable =
          m_hiveInstance.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                                  sIndexTableName,
                                  false/*bThrowException*/);
      }
      catch (HiveException e) {
          //Table not found ?
      }
    }
    return indexTable;
  }

  private List<Table> getIndexTable(Table baseTableMetaData, IndexType indexType)  {
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
        if( !sIndexType.equalsIgnoreCase(HiveIndex.IndexType.COMPACT_SUMMARY_TABLE.getName()) )  {
          continue;
        }
        vIndexTable.add(indexTable);
      } catch (HiveException e) {
      }
    }
    return vIndexTable;
  }


  private List<String> getChildColNames(ASTNode rootSelExpr)  {
    List<String> selList = new ArrayList<String>();
    for( int iChldIdx = 0; iChldIdx < rootSelExpr.getChildCount(); iChldIdx++)  {
      ASTNode childNode = (ASTNode) rootSelExpr.getChild(iChldIdx);
      if( childNode.getType() == HiveParser.TOK_SELEXPR ) {
        childNode = (ASTNode) childNode.getChild(0);

      }
      switch( childNode.getType() )  {
      case HiveParser.TOK_TABLE_OR_COL:
      {
        //COLNAME or COLNAME AS COL_ALIASE
        ASTNode internalNode = (ASTNode) childNode.getChild(0);
        selList.add(internalNode.getText());
        break;
      }
      default:
          break;
      }

    }
    return selList;
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

    //--------------------------------------------
    //Check if we have where clause, if yes that's not yet supported
    if( qbParseInfo.getWhrForClause(sClauseName) != null )  {
      getLogger().debug("Query has where clause that is not " +
      		"yet supported in rewrite " + getName());
      return false;
    }



    //--------------------------------------------
    //Getting select list column names
    ASTNode rootSelExpr = qbParseInfo.getSelForClause(sClauseName);
    boolean bIsDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
    List<String> selColNameList = getChildColNames(rootSelExpr);


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

    List<Table> vIndexTable = getIndexTable(tableQlMetaData, IndexType.COMPACT_SUMMARY_TABLE);
    if( vIndexTable.size() == 0 ) {
      getLogger().debug("Table " + sTableName + " does not have compat summary index. Cannot apply rewrite " + getName());
      return false;
    }

    Table indexTable = null;
    for( int iIdxTbl = 0;iIdxTbl < vIndexTable.size(); iIdxTbl++)  {
      indexTable = vIndexTable.get(iIdxTbl);

      //-----------------------------------------
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
      if( idxKeyColsNames.containsAll(selColNameList) == false ) {
        getLogger().debug("Select list has non index key column : " +
        		" Cannot use this index  " + indexTable.getTableName());
        continue;
      }

      //We need to check if all columns from index appear in select list only
      //in case of DISTINCT queries, In case group by queries, it is okay as long
      //as all columns from index appear in group-by-key list.
      if( bIsDistinct ) {
        //Check if all columns from index are part of select list too
        if( selColNameList.containsAll(idxKeyColsNames) == false )  {
          getLogger().debug("Index has non select list columns " +
              " Cannot use this index  " + indexTable.getTableName());
          continue;
        }
      }


      if( bIsDistinct == false )  {
        //--------------------------------------------
        //For group by, we need to check if all keys are from index columns
        //itself. Here gb key order can be different than index columns but that does
        //not really matter for final result.
        //Eg. select c1, c2 from src group by c2, c1;
        //we can rewrite this one to s
        //select c1, c2 from src_cmpt_sum_idx;
        ASTNode groupByNode = qbParseInfo.getGroupByForClause(sClauseName);
        List<String> gbKeyNameList = getChildColNames(groupByNode);
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

      }
      GbToCompactSumIdxRewriteContext rwContext = new GbToCompactSumIdxRewriteContext();
      rwContext.m_indexTableMetaData = indexTable;
      rwContext.m_sClauseName = sClauseName;
      rwContext.m_bIsDistinct = bIsDistinct;
      rwContext.m_sOrigBaseTableAliase = sTableAlias;
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
    oldQb.replaceTableAlias(rwContext.m_sOrigBaseTableAliase,
        sIndexTableName/*aliase*/,
        sIndexTableName/*tableName*/,
        sClauseName/*clauseName*/);
    qbMetaData.setSrcForAlias(sIndexTableName, indexTable);

    if( rwContext.m_bIsDistinct ) {
      //Remove distinct
      qbParseInfo.clearDistinctFlag(sClauseName);
    }
    else  {
      //Remove groupby
      qbParseInfo.clearGroupBy(sClauseName);
    }
    return oldQb;
  }

}
