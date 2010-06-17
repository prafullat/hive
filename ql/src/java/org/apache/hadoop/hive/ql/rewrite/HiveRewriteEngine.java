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
import org.apache.hadoop.hive.ql.rewrite.rules.GbToCompactSumIdxRewrite;
import org.apache.hadoop.hive.ql.rewrite.rules.HiveRwRule;
import org.apache.hadoop.hive.ql.rewrite.rules.HiveRwRuleContext;

/**
 *
 * HiveRewriteEngine.
 * Query rewrite engine for Hive QL
 *
 */
public class HiveRewriteEngine {

  private static final Log LOG = LogFactory.getLog("hive.ql.rewrite");
  private final LinkedList<HiveRwRule> m_rwRules;
  private Hive m_hiveDb;
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
      HiveRwRuleContext rwContext = null;
      LOG.debug("Trying " + m_rwRules.get(iIdx).getName() + " rewrite");

      if( m_rwRules.get(iIdx).canApplyThisRule(topQueryBlock) ) {
        LOG.debug("Applying " + m_rwRules.get(iIdx).getName() + " rewrite");
        //LOG.debug("Query block before rewrite : " + topQueryBlock.print(msg))
        QB newRewrittenQb = m_rwRules.get(iIdx).rewriteQb(topQueryBlock);
        //If rewrites have modified qb,replace our local variable
        if( null != newRewrittenQb ) {
          topQueryBlock = newRewrittenQb;
        //LOG.debug("Query block after rewrite : " + topQueryBlock.print(msg))
        }
      }
    }
    return topQueryBlock;
  }


  /**
   * Initialises rewrite engine.
   * Adds rewrite rules to m_rwRules
   */
  private void init()  {
    GbToCompactSumIdxRewrite gbToSumIdxRw = new GbToCompactSumIdxRewrite(m_hiveDb);
    m_rwRules.add(gbToSumIdxRw);
  }

  private HiveRewriteEngine(Hive hiveDb)  {
    m_rwRules = new LinkedList<HiveRwRule>();
    m_hiveDb = hiveDb;
  }


  public void setHiveDb(Hive db) {
    m_hiveDb = db;

  }
}
