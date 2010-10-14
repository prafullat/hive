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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *
 * Base class that defines interface for a Hive query rewrite rule.
 *
 */
public abstract class HiveRwRule {
  // XTODO: Does this need to be static?
  private static Log logger;
  private HiveRwRuleContext rwContext;

  public HiveRwRule(Log log) {
    logger = log;
  }

  public abstract String getName();
  public abstract boolean canApplyThisRule(QB qb);
  /**
   * Code that rewrites Qb to something new.
   * If this method returns NULL, that particular QB is removed from
   * QueryBlock tree, it must return non-null QB unless we want to
   * delete QB. If it return non-null QB which is not same as input QB,
   * we replace the input QB with it.   *
   *
   * @param oldQb Query block to rewrite.
   * @return
   */
  public abstract QB rewriteQb(QB oldQb);
  /**
   * If this method returns true, this rule needs to be
   * applied in top-down manner for QB
   * Other wise apply this in bottom-up.
   * @return
   */
  public abstract boolean applyTopDown();

  public void setContext(HiveRwRuleContext rwContext)  {
    this.rwContext = rwContext;
  }

  public HiveRwRuleContext getContext()  {
    return rwContext;
  }

  public boolean getRwFlag(HiveConf.ConfVars confVar)  {
    HiveConf hiveConf = SessionState.get().getConf();
    return HiveConf.getBoolVar(hiveConf, confVar);
  }

  Log getLogger()  {
    return logger;
  }
}
