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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.session.SessionState;

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

  private final Hive m_HiveDb;


  public GbToCompactSumIdxRewrite(Hive hiveDb) {
    m_HiveDb = hiveDb;
  }
  class GbToCompactSumIdxRewriteContext extends  HiveRwRuleContext {
    public Table m_indexTableMetaData;
    public String m_sClauseName;
    public boolean m_bIsDistinct;
    public String m_sOrigBaseTableAliase;

  }

  //FIXME: Read from metadata. Metadata currently does not have methods
  //to read from
  private Table getIndexTable(String sBaseTableName)  {
    List<String> indexSuffixList = new ArrayList<String>();
    Table indexTable = null;
    indexSuffixList.add("_proj_idx");
    indexSuffixList.add("_sum_idx");
    indexSuffixList.add("_cmpt_sum_idx");
    for(int i = 0; i < indexSuffixList.size(); i++)  {
      String sIndexTableName = sBaseTableName + indexSuffixList.get(i);
      try {
        indexTable = m_HiveDb.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, sIndexTableName, false/*bThrowException*/);
      }
      catch (HiveException e) {
          //Table not found ?
      }
    }

    return indexTable;
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

    HiveConf hiveConf = SessionState.get().getConf();

    if( HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_RW_GB_TO_IDX) == false ) {
      return false;
    }
    //Multiple table not supported yet
    if( (qb.getTabAliases().size() != 1) ||
        (qb.getSubqAliases().size() != 0) ) {
      return false;
    }


    //--------------------------------------------
    //Get Index information.
    Set<String> tableAlisesSet = qb.getTabAliases();
    Iterator<String> tableAliasesItr = tableAlisesSet.iterator();
    String sTableAlias = tableAliasesItr.next();
    String sTableName = qb.getTabNameForAlias(sTableAlias);
    Table tableQlMetaData = qb.getMetaData().getTableForAlias(sTableAlias);
    Table indexTable = getIndexTable(sTableName);
    if( indexTable == null ) {
      return false;
    }
    if( indexTable.getTableName().endsWith("_cmpt_sum_idx") == false ) {
        return false;
    }

    /* FIXME: These metadata methods do not work.
    if( MetaStoreUtils.getIndexType(indexTable.getTTable())
        != HiveIndex.IndexType.COMPACT_SUMMARY_TABLE.getName() )  {
      return false;
    }

    if( MetaStoreUtils.getBaseTableNameOfIndexTable(indexTable.getTTable())
        != sTableName ) {
      return false;
    }
    */

    String sIndexTableName = tableQlMetaData.getIndexTableName();
    List<FieldSchema> vCols = indexTable.getCols();
    Set<String> idxKeyColsNames = new TreeSet<String>();
    for( int i = 0;i < vCols.size(); i++) {
      //TODO: Skip index-specific columns eg. __bucketName __offsets etc.
      idxKeyColsNames.add(vCols.get(i).getName());
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
    //Getting select list column names
    ASTNode rootSelExpr = qbParseInfo.getSelForClause(sClauseName);
    boolean bIsDistinct = (rootSelExpr.getType() == HiveParser.TOK_SELECTDI);
    List<String> selColNameList = getChildColNames(rootSelExpr);

    //--------------------------------------------
    //Check if all columns in select list are part of index key columns
    for( int i = 0; i < selColNameList.size(); i++)  {
      if( idxKeyColsNames.contains(selColNameList.get(i)) == false ) {
          return false;
        }
    }

    if( bIsDistinct == false )  {
      //--------------------------------------------
      //For group by, we need to check if all keys are from index columns
      //itself.
      ASTNode groupByNode = qbParseInfo.getGroupByForClause(sClauseName);
      List<String> gbKeyNameList = getChildColNames(groupByNode);
      for( int i = 0; i < gbKeyNameList.size(); i++)  {
        if( idxKeyColsNames.contains(gbKeyNameList.get(i)) == false ) {
            return false;
          }
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

    oldQb.replaceTableAlias(rwContext.m_sOrigBaseTableAliase,
        sIndexTableName/*aliase*/,
        sIndexTableName/*tableName*/);
    qbParseInfo.replaceTable(rwContext.m_sOrigBaseTableAliase, sIndexTableName, sClauseName);





    QBMetaData qbMetaData = oldQb.getMetaData();


    //Query now reads from index table
    oldQb.setTabAlias(sIndexTableName, sIndexTableName);
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
