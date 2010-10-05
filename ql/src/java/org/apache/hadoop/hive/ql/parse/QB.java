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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

/**
 * Implementation of the query block.
 *
 **/

public class QB {

  private static final Log LOG = LogFactory.getLog("hive.ql.parse.QB");

  private final int numJoins = 0;
  private final int numGbys = 0;
  private int numSels = 0;
  private int numSelDi = 0;
  private HashMap<String, String> aliasToTabs;
  private HashMap<String, QBExpr> aliasToSubq;
  private QBParseInfo qbp;
  private QBMetaData qbm;
  private QBJoinTree qbjoin;
  private String id;
  private boolean isQuery;
  private CreateTableDesc tblDesc = null; // table descriptor of the final

  // results

  public void print(String msg) {
    LOG.info(msg + "alias=" + qbp.getAlias());
    for (String alias : getSubqAliases()) {
      QBExpr qbexpr = getSubqForAlias(alias);
      LOG.info(msg + "start subquery " + alias);
      qbexpr.print(msg + " ");
      LOG.info(msg + "end subquery " + alias);
    }
  }

  public QB() {
  }

  public QB(String outer_id, String alias, boolean isSubQ) {
    aliasToTabs = new HashMap<String, String>();
    aliasToSubq = new HashMap<String, QBExpr>();
    if (alias != null) {
      alias = alias.toLowerCase();
    }
    qbp = new QBParseInfo(alias, isSubQ);
    qbm = new QBMetaData();
    id = (outer_id == null ? alias : outer_id + ":" + alias);
  }

  public QBParseInfo getParseInfo() {
    return qbp;
  }

  public QBMetaData getMetaData() {
    return qbm;
  }

  public void setQBParseInfo(QBParseInfo qbp) {
    this.qbp = qbp;
  }

  public void countSelDi() {
    numSelDi++;
  }

  public void countSel() {
    numSels++;
  }

  public boolean exists(String alias) {
    alias = alias.toLowerCase();
    if (aliasToTabs.get(alias) != null || aliasToSubq.get(alias) != null) {
      return true;
    }

    return false;
  }

  public void setTabAlias(String alias, String tabName) {
    aliasToTabs.put(alias.toLowerCase(), tabName);
  }

  public void setSubqAlias(String alias, QBExpr qbexpr) {
    aliasToSubq.put(alias.toLowerCase(), qbexpr);
  }

  public String getId() {
    return id;
  }

  public int getNumGbys() {
    return numGbys;
  }

  public int getNumSelDi() {
    return numSelDi;
  }

  public int getNumSels() {
    return numSels;
  }

  public int getNumJoins() {
    return numJoins;
  }

  public Set<String> getSubqAliases() {
    return aliasToSubq.keySet();
  }

  public Set<String> getTabAliases() {
    return aliasToTabs.keySet();
  }

  public QBExpr getSubqForAlias(String alias) {
    return aliasToSubq.get(alias.toLowerCase());
  }

  public String getTabNameForAlias(String alias) {
    return aliasToTabs.get(alias.toLowerCase());
  }

  public void rewriteViewToSubq(String alias, String viewName, QBExpr qbexpr) {
    alias = alias.toLowerCase();
    String tableName = aliasToTabs.remove(alias);
    assert (viewName.equals(tableName));
    aliasToSubq.put(alias, qbexpr);
  }

  public QBJoinTree getQbJoinTree() {
    return qbjoin;
  }

  public void setQbJoinTree(QBJoinTree qbjoin) {
    this.qbjoin = qbjoin;
  }

  public void setIsQuery(boolean isQuery) {
    this.isQuery = isQuery;
  }

  public boolean getIsQuery() {
    return isQuery;
  }

  public boolean isSelectStarQuery() {
    return qbp.isSelectStarQuery() && aliasToSubq.isEmpty() && !isCTAS() && !qbp.isAnalyzeCommand();
  }

  public CreateTableDesc getTableDesc() {
    return tblDesc;
  }

  public void setTableDesc(CreateTableDesc desc) {
    tblDesc = desc;
  }

  /**
   * Whether this QB is for a CREATE-TABLE-AS-SELECT.
   */
  public boolean isCTAS() {
    return tblDesc != null;
  }

  public void removeTable(String sAliasName)  {
    aliasToTabs.remove(sAliasName);
  }

  public void replaceTableAlias(String sOrigBaseTableAlias, String sAlias,
      String sTableName, String sClauseName) {
    removeTable(sOrigBaseTableAlias);
    setTabAlias(sAlias, sTableName);
    qbp.replaceTable(sOrigBaseTableAlias, sAlias, sClauseName);
  }

  public void removeSubQuery(String sAliasName)  {
    aliasToSubq.remove(sAliasName);
  }

  public void replaceSubQuery(String sOrigSubQAlias, String sNewAlias, QBExpr newQbExpr)  {
    removeSubQuery(sOrigSubQAlias);
    setSubqAlias(sNewAlias, newQbExpr);
  }



  public ASTNode newSelectListExpr(boolean bHasFunc, String sFuncName,
        List<String> vInput)  {
    ASTNode selListExpr = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
    if( bHasFunc == true )  {
      ASTNode funcNode = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION"));
      funcNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, sFuncName)));

      for( int i = 0 ; i < vInput.size(); i++ )  {
        ASTNode colRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));
        colRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, vInput.get(i))));
        funcNode.addChild(colRefNode);
      }
      selListExpr.addChild(funcNode);
    }
    else  {
      ASTNode colRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));
      colRefNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, vInput.get(0))));
      selListExpr.addChild(colRefNode);
    }
    return selListExpr;
  }

  public ASTNode newLateralView(String sAliasName, String sViewName, String sInputToExplode)  {
    ASTNode selExpr = new ASTNode(new CommonToken(HiveParser.TOK_SELECT, "TOK_SELECT"));
    List<String> vInput = new ArrayList<String>();
    vInput.add(sInputToExplode);

    ASTNode funcSelExprNode = newSelectListExpr(true, "explode", vInput);
    int viewNum = 1;

    funcSelExprNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, "_lt_view_" + viewNum )));
    ASTNode tokTabAlias = new ASTNode(new CommonToken(HiveParser.TOK_TABALIAS,"TOK_TABALIAS"));
    tokTabAlias.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, "_lt_view_alias_" + viewNum)));
    funcSelExprNode.addChild(tokTabAlias);
    selExpr.addChild(funcSelExprNode);
    ASTNode lateralView = new ASTNode(new CommonToken(HiveParser.TOK_LATERAL_VIEW, "TOK_LATERAL_VIEW"));
    lateralView.addChild(selExpr);
    return lateralView;

  }

  public void addLateralView(ASTNode lateralViewAstNode) {

  }
}
