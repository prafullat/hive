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
  abstract public QB rewriteQb(QB oldQb);

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
