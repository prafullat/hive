package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public final class ParseContextGenerator {
  protected static Log LOG = LogFactory.getLog(RewriteIndexSubqueryProcFactory.class.getName());
  private ParseContext parseContext = null;

  public ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  public ParseContextGenerator() {
    // TODO Auto-generated constructor stub
  }

  public ParseContext generateDAGForSubquery(String command){
    HiveConf conf = parseContext.getConf();
    Context ctx;
    ParseContext subPCtx = null;
    try {
      ctx = new Context(conf);
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, ctx);
    tree = ParseUtils.findRootNonNullToken(tree);

    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
    doSemanticAnalysis(ctx, sem, tree);

    subPCtx = ((SemanticAnalyzer) sem).getParseContext();
    LOG.info("Sub-query Semantic Analysis Completed");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return subPCtx;

  }

  @SuppressWarnings("unchecked")
  private Operator<Serializable> doSemanticAnalysis(Context ctx, BaseSemanticAnalyzer sem, ASTNode ast) throws SemanticException {

    if(sem instanceof SemanticAnalyzer){
      QB qb = new QB(null, null, false);
      ASTNode child = ast;
      ParseContext subPCtx = ((SemanticAnalyzer) sem).getParseContext();
      subPCtx.setContext(ctx);
      ((SemanticAnalyzer) sem).init(subPCtx);


      LOG.info("Starting Sub-query Semantic Analysis");
      ((SemanticAnalyzer) sem).doPhase1(child, qb, ((SemanticAnalyzer) sem).initPhase1Ctx());
      LOG.info("Completed phase 1 of Sub-query Semantic Analysis");

      ((SemanticAnalyzer) sem).getMetaData(qb);
      LOG.info("Completed getting MetaData in Sub-query Semantic Analysis");

      LOG.info("Sub-query Abstract syntax tree: " + ast.toStringTree());
      Operator<Serializable> sinkOp = ((SemanticAnalyzer) sem).genPlan(qb);

      //LOG.info("Processing for Sub-query = " + sinkOp.getName() + "(" + ((Operator<Serializable>) sinkOp).getIdentifier() + ")");
      LOG.info("Sub-query Completed plan generation");
       return sinkOp;

    } else {
      return null;
    }

  }

}
