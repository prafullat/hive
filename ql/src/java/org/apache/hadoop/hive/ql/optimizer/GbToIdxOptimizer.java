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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 *
 * Implements GroupBy clause rewrite using compact index.
 * This optimization rewrites GroupBy query over base table to the query over simple table-scan over
 * index table, if there is index on the group by key(s) or the distinct column(s).
 * E.g.
 * <code>
 *   select key
 *   from table
 *   group by key;
 * </code>
 *  to
 *  <code>
 *   select key
 *   from idx_table;
 *  </code>
 *
 *  The rewrite supports following queries
 *  - Queries having only those col refs that are in the index key.
 *  - Queries that have index key col refs
 *    - in SELECT
 *    - in WHERE
 *    - in GROUP BY
 *  - Queries with agg func COUNT(literal) or COUNT(index key col ref)
 *    in SELECT
 *  - Queries with SELECT DISTINCT index key col refs
 *  - Queries having a subquery satisfying above condition (only the
 *    subquery is rewritten)
 *
 *  FUTURE:
 *  - Many of the checks for above criteria rely on equivalence of
 *    expressions, but such framework/mechanism of expression equivalence
 *    isn't present currently or developed yet. This needs to be supported
 *    in order for better robust checks. This is critically important for
 *    correctness of a query rewrite system.
 *    - Also this code currently directly works on the parse tree data
 *      structs (AST nodes) for checking, manipulating query data structure.
 *      If such expr equiv mechanism is to be developed, it would be important
 *      to think and reflect on whether to continue use the parse tree
 *      data structs (and enhance those classes with such equivalence methods)
 *      or to create independent hierarchies of data structs and classes
 *      for the exprs and develop that equivalence mechanism on that new
 *      class hierarchy, code.
 *
 * @see org.apache.hadoop.hive.ql.index.HiveIndex
 * @see org.apache.hadoop.hive.ql.index.HiveIndex.CompactIndexHandler
 *
 */
public class GbToIdxOptimizer implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private GbToIdxContext gbToIdxContext;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    parseContext = pctx;
    opToParseCtxMap = parseContext.getOpParseCtx();
    try {
      hiveDb = Hive.get(parseContext.getConf());
    } catch (HiveException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }


    gbToIdxContext = new GbToIdxContext();
    gbToIdxContext.indexTableName = "something";
    gbToIdxContext.hiveDb = hiveDb;
    gbToIdxContext.parseContext = parseContext;




    if( shouldApplyOptimization(parseContext) == false ) {
      return null;
    }

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "TS%"), new GbToIdxTableScanProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new GbToIdxDefaultProc(), opRules, gbToIdxContext);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);



    // TODO Auto-generated method stub
    return null;
  }

  protected boolean shouldApplyOptimization(ParseContext parseContext)  {
    return true;
  }

  class GbToIdxContext implements NodeProcessorCtx {
    private String indexTableName;
    private ParseContext parseContext;
    private Hive hiveDb;


  }

  public static class GbToIdxDefaultProc implements NodeProcessor {

     //No-Op
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      return null;
    }


  }


  public static class GbToIdxTableScanProc implements NodeProcessor {

    public void setUpTableDesc(TableScanDesc tsDesc,Table table, String alias)  {

      tsDesc.setGatherStats(false);

      String tblName = table.getTableName();
      tableSpec tblSpec = createTableScanDesc(table, alias);
      String k = tblName + Path.SEPARATOR;
      tsDesc.setStatsAggPrefix(k);
    }

    private tableSpec createTableScanDesc(Table table, String alias) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      GbToIdxContext context = (GbToIdxContext)procCtx;


      TableScanOperator scanOperator = (TableScanOperator)nd;
      //Get the lineage information corresponding to this
      //and modify it ?
      TableScanDesc indexTableDesc = new TableScanDesc();
      //Modify outputs




      return null;
    }
  }


  public tableSpec getTableSpec(String aliase) {
    //gbToIdxContext.hiveDb;
    //gbToIdxContext.parseContext.getConf()
    //tableSpec ts = new tableSpec();

    //tableName
    //tableHandle
    //No part spec
    //no part handle?
    //Spec type set to TABLE_ONLY
    return null;
  }
}


