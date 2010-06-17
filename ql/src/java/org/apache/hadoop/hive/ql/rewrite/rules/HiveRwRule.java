package org.apache.hadoop.hive.ql.rewrite.rules;

import org.apache.hadoop.hive.ql.parse.QB;

public abstract class HiveRwRule {
  abstract public String getName();
  abstract public boolean canApplyThisRule(QB qb);
  abstract public QB rewriteQb(QB oldQb);

  public void setContext(HiveRwRuleContext rwContext)  {
    m_rwContext = rwContext;
  }

  public HiveRwRuleContext getContext()  {
    return m_rwContext;
  }

  private HiveRwRuleContext m_rwContext;
}
