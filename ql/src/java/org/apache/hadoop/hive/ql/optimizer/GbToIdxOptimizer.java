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

import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * Implements optimizations for GroupBy clause rewrite using compact index.
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

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // TODO Auto-generated method stub
    return null;
  }

}
