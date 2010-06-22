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
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.rewrite.rules.GbToCompactSumIdxRewrite;
import org.apache.hadoop.hive.ql.rewrite.rules.HiveRwRule;

/**
 *
 * HiveRewriteEngine.
 * Query rewrite engine for Hive QL
 *
 */
public class HiveRewriteEngine {

  private static final Log LOG = LogFactory.getLog("hive.ql.rewrite");
  private final LinkedList<HiveRwRule> m_rwRules;
  private Hive m_hiveInstance;
  private static HiveRewriteEngine stc_Instance;

  public static HiveRewriteEngine getInstance(Hive hiveDb)  {
    if( stc_Instance == null ) {
      stc_Instance = new HiveRewriteEngine(hiveDb);
      stc_Instance.init();
    }
    return stc_Instance;
  }


  public QB invokeRewrites(QB topQueryBlock)  {
    LOG.debug("Invoking rewrites on QB(Id "+topQueryBlock.getId()+")");
    for(int iIdx = 0; iIdx < m_rwRules.size(); iIdx++) {
      QB newRewrittenQb = null;
      if( m_rwRules.get(iIdx).applyTopDown() == false )  {
        newRewrittenQb = invokeRewriteInBottomUpWay(topQueryBlock, m_rwRules.get(iIdx));
      }
      else  {
        newRewrittenQb = invokeRewriteInTopDownWay(topQueryBlock, m_rwRules.get(iIdx));
      }
      if( null != newRewrittenQb ) {
        topQueryBlock = newRewrittenQb;
      }
    }
    return topQueryBlock;
  }

  public QB invokeRewriteInTopDownWay(QB inputQb, HiveRwRule hiveRwRule)  {
    //Apply the rewrite on top QB
    QB newRewrittenQb = applyRewrite(hiveRwRule, inputQb);
    if( null != newRewrittenQb )  {
      inputQb = newRewrittenQb;
    }
    if( newRewrittenQb == null  )  {
      inputQb = null;
      return inputQb;
    }

    for( String sSubQueryAlias : inputQb.getSubqAliases() )  {
      QB childSubQueryQb = inputQb.getSubqForAlias(sSubQueryAlias).getQB();
      QB newQb = invokeRewriteInTopDownWay(childSubQueryQb, hiveRwRule);
      if( newQb == null )  {
        inputQb.removeSubQuery(sSubQueryAlias);
      }
      else if( newQb != childSubQueryQb ) {
        QBExpr qbExpr = new QBExpr(newQb);
        inputQb.replaceSubQuery(sSubQueryAlias, sSubQueryAlias, qbExpr);
      }

    }
    return inputQb;
  }


  public QB invokeRewriteInBottomUpWay(QB inputQb, HiveRwRule hiveRwRule)  {
    for( String sSubQueryAlias : inputQb.getSubqAliases() )  {
      QB childSubQueryQb = inputQb.getSubqForAlias(sSubQueryAlias).getQB();
      QB newQb = invokeRewriteInBottomUpWay(childSubQueryQb, hiveRwRule);
      if( newQb == null )  {
        inputQb.removeSubQuery(sSubQueryAlias);
      }
      else if( newQb != childSubQueryQb ) {
        QBExpr qbExpr = new QBExpr(newQb);
        inputQb.replaceSubQuery(sSubQueryAlias, sSubQueryAlias, qbExpr);
      }
    }
    return applyRewrite(hiveRwRule, inputQb);
  }

  public QB applyRewrite(HiveRwRule hiveRwRule, QB inputQb)  {
    LOG.debug("Trying " + hiveRwRule.getName() + " rewrite");
    QB newRewrittenQb = null;
    if( hiveRwRule.canApplyThisRule(inputQb) ) {
      LOG.debug("Applying " + hiveRwRule.getName() + " rewrite");
      newRewrittenQb = hiveRwRule.rewriteQb(inputQb);
      //If rewrites have modified qb,replace our local variable
      LOG.debug("Done with rewrite " + hiveRwRule.getName());
    }
    return newRewrittenQb;
  }


  /**
   * Initialises rewrite engine.
   * Adds rewrite rules to m_rwRules
   */
  private void init()  {
    GbToCompactSumIdxRewrite gbToSumIdxRw = new GbToCompactSumIdxRewrite(m_hiveInstance, LOG);
    m_rwRules.add(gbToSumIdxRw);
  }

  private HiveRewriteEngine(Hive hiveInstance)  {
    m_rwRules = new LinkedList<HiveRwRule>();
    m_hiveInstance = hiveInstance;
  }


  public void setHiveDb(Hive hiveInstance) {
    m_hiveInstance = hiveInstance;

  }
}
