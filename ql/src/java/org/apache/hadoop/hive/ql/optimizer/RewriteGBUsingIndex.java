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
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class RewriteGBUsingIndex implements Transform {
  private ParseContext parseContext;
  private Hive hiveDb;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

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

    if( shouldApplyOptimization(parseContext) == false ) {
      return parseContext;
    }

    return null;
  }

  protected boolean shouldApplyOptimization(ParseContext parseContext)  {
    QB inputQb = parseContext.getQB();
    boolean retValue = true;



    for (String subqueryAlias : inputQb.getSubqAliases()) {
      QB childSubQueryQb = inputQb.getSubqForAlias(subqueryAlias).getQB();
      retValue |= checkSingleQB(childSubQueryQb, new RewriteGBUsingIndexProcCtx()) ;
    }
    retValue |= checkSingleQB(inputQb,  new RewriteGBUsingIndexProcCtx()) ;
    return retValue;
  }

  private boolean checkSingleQB(QB childSubQueryQb,
      RewriteGBUsingIndexProcCtx rewriteGBUsingIndexProcCtx) {
    // TODO Auto-generated method stub
    return false;
  }

  public class RewriteGBUsingIndexProcCtx implements NodeProcessorCtx {
  }


}
