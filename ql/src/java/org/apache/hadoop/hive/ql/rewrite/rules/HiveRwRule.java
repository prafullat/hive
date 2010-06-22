package org.apache.hadoop.hive.ql.rewrite.rules;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.session.SessionState;

public abstract class HiveRwRule {
  public static Log m_stc_Log;
  private HiveRwRuleContext m_rwContext;

  public HiveRwRule(Log log) {
    m_stc_Log = log;
  }

  abstract public String getName();
  abstract public boolean canApplyThisRule(QB qb);
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
  abstract public QB rewriteQb(QB oldQb);
  /**
   * If this method returns true, this rule needs to be
   * applied in top-down manner for QB
   * Other wise apply this in bottom-up.
   * @return
   */
  abstract public boolean applyTopDown();

  public void setContext(HiveRwRuleContext rwContext)  {
    m_rwContext = rwContext;
  }

  public HiveRwRuleContext getContext()  {
    return m_rwContext;
  }

  public boolean getRwFlag(HiveConf.ConfVars confVar)  {
    HiveConf hiveConf = SessionState.get().getConf();
    return HiveConf.getBoolVar(hiveConf, confVar);
  }
  Log getLogger()  {
    return m_stc_Log;
  }
}
