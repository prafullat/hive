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

package org.apache.hadoop.hive.ql.rewrite;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.rewrite.rules.GbToCompactSumIdxRewrite;
import org.apache.hadoop.hive.ql.rewrite.rules.HiveRwRule;

/**
 *
 * Query rewrite engine for Hive. Holds a list of rules registry. For each rule, evaluates the
 * apply condition for the rule, and if it matches, invokes the rewrite method of the rule.
 * The implementation is loosely based on publicly available paper on query rewrites:
 * "Extensible/Rule Based Query Rewrite Optimization in Starburst (1992)"
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.40.7952
 *
 */
public final class HiveRewriteEngine {
  // Changing hive.root.logger var to add DEBUG in to the list will
  // show trace rewrite messages with this category. E.g. either modify
  // the hive.root.logger in conf/hive-log4j.properties or pass it
  // as -hiveconf param.
  private static final Log LOG = LogFactory.getLog("hive.ql.rewrite");
  private final LinkedList<HiveRwRule> rwRules;
  private final Hive hiveInstance;

  public QB invokeRewrites(QB topQueryBlock, ASTNode rootNode) {
    LOG.debug("Invoking rewrites on QB(Id "+topQueryBlock.getId()+")");
    // Invoke the rewrite rules in the same order as they were added to the rwRules list.
    // XTODO: Give examples of both kind of rewrites, top-down & bottom-up
    for (int idx = 0; idx < rwRules.size(); idx++) {
      HiveRwRule rwRule = rwRules.get(idx);
      QB newRewrittenQb = topQueryBlock;
      if (!rwRule.applyTopDown()) {
        newRewrittenQb = invokeRewriteInBottomUpWay(topQueryBlock, rootNode, rwRule);
      } else {
        newRewrittenQb = invokeRewriteInTopDownWay(topQueryBlock, rootNode, rwRule);
      }
      if (null != newRewrittenQb) {
        topQueryBlock = newRewrittenQb;
      }
    }
    // XTODO: Print the query block before & after rewrites for each rewrite (with rewrite name)
    return topQueryBlock;
  }

  public QB invokeRewriteInTopDownWay(QB inputQb, ASTNode rootNode, HiveRwRule hiveRwRule) {
    // Apply the rewrite on top QB
    QB newRewrittenQb = applyRewrite(hiveRwRule, inputQb, rootNode);
    if (null != newRewrittenQb) {
      inputQb = newRewrittenQb;
    }
    if (newRewrittenQb == null) {
      inputQb = null;
      return inputQb;
    }

    for (String subQueryAlias : inputQb.getSubqAliases()) {
      QB childSubQueryQb = inputQb.getSubqForAlias(subQueryAlias).getQB();
      QB newQb = invokeRewriteInTopDownWay(childSubQueryQb, rootNode, hiveRwRule);
      if (newQb == null) {
        inputQb.removeSubQuery(subQueryAlias);
      } else if (newQb != childSubQueryQb) {
        QBExpr qbExpr = new QBExpr(newQb);
        inputQb.replaceSubQuery(subQueryAlias, subQueryAlias, qbExpr);
      }
    }
    return inputQb;
  }


  public QB invokeRewriteInBottomUpWay(QB inputQb, ASTNode rootNode, HiveRwRule hiveRwRule) {
    for (String subqueryAlias : inputQb.getSubqAliases()) {
      QB childSubQueryQb = inputQb.getSubqForAlias(subqueryAlias).getQB();
      QB newQb = invokeRewriteInBottomUpWay(childSubQueryQb, rootNode, hiveRwRule);
      if (newQb == null) {
        inputQb.removeSubQuery(subqueryAlias);
      } else if (newQb != childSubQueryQb) {
        QBExpr qbExpr = new QBExpr(newQb);
        inputQb.replaceSubQuery(subqueryAlias, subqueryAlias, qbExpr);
      }
    }
    return applyRewrite(hiveRwRule, inputQb, rootNode);
  }

  public QB applyRewrite(HiveRwRule hiveRwRule, QB inputQb, ASTNode rootNode)  {
    LOG.debug("Trying " + hiveRwRule.getName() + " rewrite");
    QB newRewrittenQb = inputQb;
    if (hiveRwRule.canApplyThisRule(inputQb, rootNode)) {
      LOG.debug("Applying " + hiveRwRule.getName() + " rewrite");
      newRewrittenQb = hiveRwRule.rewriteQb(inputQb);
      //If rewrites have modified Qb, replace our local variable.
      LOG.debug("Done with rewrite " + hiveRwRule.getName());
    }
    return newRewrittenQb;
  }

  /**
   * Initializes rewrite engine.
   * Adds rewrite rules to rwRules
   */
  private void init()  {
    assert(hiveInstance != null); // hiveInstance is passed to all the rewrite rules.
    assert(LOG != null); // LOG is passsed to all the rewrite rules.
    // List all the rewrite rules here.
    GbToCompactSumIdxRewrite gbToSumIdxRw = new GbToCompactSumIdxRewrite(hiveInstance, LOG);
    rwRules.add(gbToSumIdxRw);
  }

  public HiveRewriteEngine(Hive hiveInstance)  {
    rwRules = new LinkedList<HiveRwRule>();
    this.hiveInstance = hiveInstance;
    init();
  }
}
