package org.apache.hadoop.hive.ql.optimizer;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

public class RewriteGBUsingIndexCtx  implements NodeProcessorCtx {

  //Map for base table to index table mapping
  //TableScan operator for base table will be modified to read from index table
  private final HashMap<String, String> baseToIdxTableMap;

  public RewriteGBUsingIndexCtx() {
    baseToIdxTableMap = new HashMap<String, String>();
  }

  public void addTable(String baseTableName, String indexTableName) {
    baseToIdxTableMap.put(baseTableName, indexTableName);
  }

  public String findBaseTable(String baseTableName)  {
    return baseToIdxTableMap.get(baseTableName);
  }

}
