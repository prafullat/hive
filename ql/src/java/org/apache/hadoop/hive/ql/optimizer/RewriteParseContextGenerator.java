package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
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

/**
 * RewriteParseContextGenerator is a class that offers methods to generate operator tree
 * for input queries. It is implemented on lines of the analyzeInternal(..) method
 * of {@link SemanticAnalyzer} but it creates only the ParseContext for the input query command.
 * It does not optimize or generate map-reduce tasks for the input query.
 * This can be used when you need to create operator tree for an internal query.
 * For example, {@link RewriteGBUsingIndex} uses the {@link RewriteIndexSubqueryProcFactory} methods to
 * generate subquery that scans over index table rather than original table.
 *
 */
public final class RewriteParseContextGenerator {
  protected static Log LOG = LogFactory.getLog(RewriteParseContextGenerator.class.getName());

  /**
   * Parse the input {@link String} command and generate a ASTNode tree
   * @param conf
   * @param command
   * @return
   */
  public static ParseContext generateOperatorTree(HiveConf conf, String command){
    Context ctx;
    ParseContext subPCtx = null;
    try {
      ctx = new Context(conf);
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, ctx);
    tree = ParseUtils.findRootNonNullToken(tree);

    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
    doSemanticAnalysis(sem, tree, ctx);

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

  /**
   * For the input ASTNode tree, perform a semantic analysis and check metadata
   * Generate a operator tree and return the {@link ParseContext} instance for the operator tree
   *
   * @param ctx
   * @param sem
   * @param ast
   * @return
   * @throws SemanticException
   */
  private static void doSemanticAnalysis(BaseSemanticAnalyzer sem, ASTNode ast, Context ctx) throws SemanticException {

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
      ((SemanticAnalyzer) sem).genPlan(qb);

      LOG.info("Sub-query Completed plan generation");
    }
  }

}
