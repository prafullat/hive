package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * RewriteCanApplyCtx class stores the context for the {@link RewriteCanApplyProcFactory} to determine
 * if any index can be used and if the input query meets all the criteria for rewrite optimization.
 */
public final class RewriteCanApplyCtx implements NodeProcessorCtx {

  protected final  Log LOG = LogFactory.getLog(RewriteCanApplyCtx.class.getName());

  private RewriteCanApplyCtx() {

  }

  public static RewriteCanApplyCtx getInstance(){
    return new RewriteCanApplyCtx();
  }

  public static enum RewriteVars {

   NO_OF_SUBQUERIES("hive.ql.rewrites.no.of.subqueries",0),
   AGG_FUNC_CNT("hive.ql.rewrites.agg.func.cnt", 0),
   GBY_KEY_CNT("hive.ql.rewrites.gby.key.cnt", 0),
   TABLE_HAS_NO_INDEX("hive.ql.rewrites.table.has.no.index", false),
   QUERY_HAS_SORT_BY("hive.ql.rewrites.query.has.sort.by", false),
   QUERY_HAS_ORDER_BY("hive.ql.rewrites.query.has.order.by", false),
   QUERY_HAS_DISTRIBUTE_BY("hive.ql.rewrites.query.has.distribute.by", false),
   QUERY_HAS_GROUP_BY("hive.ql.rewrites.query.has.group.by", false),
   QUERY_HAS_DISTINCT("hive.ql.rewrites.query.has.distinct", false), //This still uses QBParseInfo to make decision. Needs to be changed if QB dependency is not desired.
   AGG_FUNC_IS_NOT_COUNT("hive.ql.rewrites.agg.func.is.not.count", false),
   AGG_FUNC_COLS_FETCH_EXCEPTION("hive.ql.rewrites.agg.func.cols.fetch.exception", false),
   WHR_CLAUSE_COLS_FETCH_EXCEPTION("hive.ql.rewrites.whr.clause.cols.fetch.exception", false),
   SEL_CLAUSE_COLS_FETCH_EXCEPTION("hive.ql.rewrites.sel.clause.cols.fetch.exception", false),
   GBY_KEYS_FETCH_EXCEPTION("hive.ql.rewrites.gby.keys.fetch.exception", false),
   GBY_KEY_HAS_NON_INDEX_COLS("hive.ql.rewrites.gby.keys.has.non.index.cols", false),
   SEL_HAS_NON_COL_REF("hive.ql.rewrites.sel.has.non.col.ref", false),
   GBY_NOT_ON_COUNT_KEYS("hive.ql.rewrites.gby.not.on.count.keys", false),
   IDX_TBL_SEARCH_EXCEPTION("hive.ql.rewrites.idx.tbl.search.exception", false),
   QUERY_HAS_KEY_MANIP_FUNC("hive.ql.rewrites.query.has.key.manip.func", false),
   QUERY_HAS_MULTIPLE_TABLES("hive.ql.rewrites.query.has.multiple.tables", false),
   SHOULD_APPEND_SUBQUERY("hive.ql.rewrites.should.append.subquery", false),
   REMOVE_GROUP_BY("hive.ql.rewrites.remove.group.by", false);
    ;

    public final String varname;
    public final int defaultIntVal;
    public final boolean defaultBoolVal;
    public final Class<?> valClass;

    //Constructors for int and boolean values
    RewriteVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultIntVal = defaultIntVal;
      this.defaultBoolVal = false;
    }

    RewriteVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultIntVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }

    @Override
    public String toString() {
      return varname;
    }



  }

  /*
   * Methods to set and retrieve the RewriteVars enum variables
   * */
  public int getIntVar(Configuration conf, RewriteVars var) {
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public void setIntVar(Configuration conf, RewriteVars var, int val) {
    assert (var.valClass == Integer.class);
    conf.setInt(var.varname, val);
  }

  public boolean getBoolVar(Configuration conf, RewriteVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public void setBoolVar(Configuration conf, RewriteVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    conf.setBoolean(var.varname, val);
  }

  public void resetRewriteVars(){
    setIntVar(conf, RewriteVars.NO_OF_SUBQUERIES,0);
    setIntVar(conf, RewriteVars.AGG_FUNC_CNT,0);
    setIntVar(conf, RewriteVars.GBY_KEY_CNT,0);
    setBoolVar(conf, RewriteVars.TABLE_HAS_NO_INDEX, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_SORT_BY, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_ORDER_BY, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_DISTRIBUTE_BY, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_GROUP_BY, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_DISTINCT, false);
    setBoolVar(conf, RewriteVars.AGG_FUNC_IS_NOT_COUNT, false);
    setBoolVar(conf, RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION, false);
    setBoolVar(conf, RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION, false);
    setBoolVar(conf, RewriteVars.SEL_CLAUSE_COLS_FETCH_EXCEPTION, false);
    setBoolVar(conf, RewriteVars.GBY_KEYS_FETCH_EXCEPTION, false);
    setBoolVar(conf, RewriteVars.GBY_KEY_HAS_NON_INDEX_COLS, false);
    setBoolVar(conf, RewriteVars.SEL_HAS_NON_COL_REF, false);
    setBoolVar(conf, RewriteVars.GBY_NOT_ON_COUNT_KEYS, false);
    setBoolVar(conf, RewriteVars.IDX_TBL_SEARCH_EXCEPTION, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_KEY_MANIP_FUNC, false);
    setBoolVar(conf, RewriteVars.QUERY_HAS_MULTIPLE_TABLES, false);
    setBoolVar(conf, RewriteVars.SHOULD_APPEND_SUBQUERY, false);
    setBoolVar(conf, RewriteVars.REMOVE_GROUP_BY, false);
  }



  /***************************************Index Validation Variables***************************************/
  //The SUPPORTED_INDEX_TYPE value will change when we implement a new index handler to retrieve correct result
  // for count if the same key appears more than once within the same block
  final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
   final String COMPACT_IDX_BUCKET_COL = "_bucketname";
   final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";

   //Data structures that are populated in the RewriteCanApplyProcFactory methods to check if the index key meets all criteria
   final Set<String> selColRefNameList = new LinkedHashSet<String>();
   final List<String> predColRefs = new ArrayList<String>();
   final List<String> gbKeyNameList = new ArrayList<String>();
   final List<List<String>> colRefAggFuncInputList = new ArrayList<List<String>>();

   //Map for base table to index table mapping
   //TableScan operator for base table will be modified to read from index table
   private final HashMap<String, String> baseToIdxTableMap = new HashMap<String, String>();;
   private HiveConf conf = null;
   private int aggFuncCnt = 0;
   private  ParseContext parseContext = null;
   private Hive hiveDb;
   private String currentTableName = null;


  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

   public int getAggFuncCnt() {
    return aggFuncCnt;
  }

  public void setAggFuncCnt(int aggFuncCnt) {
    this.aggFuncCnt = aggFuncCnt;
  }

  public void addTable(String baseTableName, String indexTableName) {
     baseToIdxTableMap.put(baseTableName, indexTableName);
   }

   public String findBaseTable(String baseTableName)  {
     return baseToIdxTableMap.get(baseTableName);
   }

  public Hive getHiveDb() {
    return hiveDb;
  }

  public void setHiveDb(Hive hiveDb) {
    this.hiveDb = hiveDb;
  }

  public String getCurrentTableName() {
    return currentTableName;
  }

  public void setCurrentTableName(String currentTableName) {
    this.currentTableName = currentTableName;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }


  /**
   * This block of code iterates over the topToTable map from ParseContext
   * to determine if the query has a scan over multiple tables.
   * @return
   */
  boolean ifQueryHasMultipleTables(){
    HashMap<TableScanOperator, Table> topToTable = parseContext.getTopToTable();
    Iterator<Table> valuesItr = topToTable.values().iterator();
    Set<String> tableNameSet = new HashSet<String>();
    while(valuesItr.hasNext()){
      Table table = valuesItr.next();
      tableNameSet.add(table.getTableName());
    }
    if(tableNameSet.size() > 1){
      setBoolVar(parseContext.getConf(), RewriteVars.QUERY_HAS_MULTIPLE_TABLES, true);
      return true;
    }
    return false;
  }

  /**
   * This method walks all the nodes starting from topOp TableScanOperator node
   * and invokes methods from {@link RewriteCanApplyProcFactory} for each of the rules
   * added to the opRules map. We use the {@link DefaultGraphWalker} for a post-order
   * traversal of the operator tree.
   *
   * The methods from {@link RewriteCanApplyProcFactory} set appropriate values in
   * {@link RewriteVars} enum.
   *
   * @param topOp
   */
  void populateRewriteVars(Operator<? extends Serializable> topOp){
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "FIL%"), RewriteCanApplyProcFactory.canApplyOnFilterOperator());
    opRules.put(new RuleRegExp("R2", "GBY%"), RewriteCanApplyProcFactory.canApplyOnGroupByOperator());
    opRules.put(new RuleRegExp("R3", "OP%"), RewriteCanApplyProcFactory.canApplyOnExtractOperator());
    opRules.put(new RuleRegExp("R4", "SEL%"), RewriteCanApplyProcFactory.canApplyOnSelectOperator());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }


  /**
   * Default procedure for {@link DefaultRuleDispatcher}
   * @return
   */
  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }


   /**
   * Given a base table meta data, and a list of index types for which we need to find a matching index,
   * this method returns a list of matching index tables.
   * @param baseTableMetaData
   * @param matchIndexTypes
   * @return
   */
  List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes) {
    List<Index> matchingIndexes = new ArrayList<Index>();
    List<Index> indexesOnTable = null;

    try {
      short maxNumOfIndexes = 1024; // XTODO: Hardcoding. Need to know if
      // there's a limit (and what is it) on
      // # of indexes that can be created
      // on a table. If not, why is this param
      // required by metastore APIs?
      indexesOnTable = baseTableMetaData.getAllIndexes(maxNumOfIndexes);

    } catch (HiveException e) {
      return matchingIndexes; // Return empty list (trouble doing rewrite
      // shouldn't stop regular query execution,
      // if there's serious problem with metadata
      // or anything else, it's assumed to be
      // checked & handled in core hive code itself.
    }

    for (int i = 0; i < indexesOnTable.size(); i++) {
      Index index = null;
      index = indexesOnTable.get(i);
      // The handler class implies the type of the index (e.g. compact
      // summary index would be:
      // "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler").
      String indexType = index.getIndexHandlerClass();
      for (int  j = 0; j < matchIndexTypes.size(); j++) {
        if (indexType.equals(matchIndexTypes.get(j))) {
          matchingIndexes.add(index);
          break;
        }
      }
    }
    return matchingIndexes;
  }


  /**
   * We retrieve the list of index tables on the current table (represented by the TableScanOperator)
   * and return if there are no index tables to be used for rewriting the input query.
   * Else, we walk the operator tree for which this TableScanOperator is the topOp.
   * At the end, we check if all conditions have passed for rewrite. If yes, we
   * determine if the the index is usable for rewrite. Else, we log the condition which
   * did not meet the rewrite criterion.
   *
   * @param topOp
   * @return
   */
  HashMap<String, Set<String>> getIndexTableInfoForRewrite(TableScanOperator topOp) {
    HashMap<String, Set<String>> indexTableMap = null;
    TableScanOperator ts = (TableScanOperator) topOp;
    Table tsTable = parseContext.getTopToTable().get(ts);
    if (tsTable != null) {
      List<String> idxType = new ArrayList<String>();
      idxType.add(SUPPORTED_INDEX_TYPE);
      List<Index> indexTables = getIndexes(tsTable, idxType);
      if (indexTables.size() == 0) {
        setBoolVar(parseContext.getConf(), RewriteVars.TABLE_HAS_NO_INDEX, true);
      }else{
        indexTableMap = populateIndexToKeysMap(indexTables);
      }
    }
    return indexTableMap;
  }


  /**
   * This code block iterates over indexes on the table and picks
   * up the first index that satisfies the rewrite criteria.
   * @param indexTables
   * @return
   */
  HashMap<String, Set<String>> populateIndexToKeysMap(List<Index> indexTables){
    Index index = null;
    Hive hiveInstance = hiveDb;
    HashMap<String, Set<String>> indexToKeysMap = new LinkedHashMap<String, Set<String>>();

    ArrayList<String> unusableIndexNames = new ArrayList<String>();
    for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      String indexTableName = "";
      final Set<String> indexKeyNames = new LinkedHashSet<String>();
      boolean removeGroupBy = true;
      boolean optimizeCount = false;

      index = indexTables.get(idxCtr);
      indexTableName = index.getIndexTableName();

      //Getting index key columns
      StorageDescriptor sd = index.getSd();
      List<FieldSchema> idxColList = sd.getCols();
      for (FieldSchema fieldSchema : idxColList) {
        indexKeyNames.add(fieldSchema.getName());
      }


      // Check that the index schema is as expected. This code block should
      // catch problems of this rewrite breaking when the CompactIndexHandler
      // index is changed.
      // This dependency could be better handled by doing init-time check for
      // compatibility instead of this overhead for every rewrite invocation.
      ArrayList<String> idxTblColNames = new ArrayList<String>();
      try {
        Table idxTbl = hiveInstance.getTable(index.getDbName(),
            index.getIndexTableName());
        for (FieldSchema idxTblCol : idxTbl.getCols()) {
          idxTblColNames.add(idxTblCol.getName());
        }
      } catch (HiveException e) {
        setBoolVar(conf, RewriteVars.IDX_TBL_SEARCH_EXCEPTION, true);
        return indexToKeysMap;
      }
      assert(idxTblColNames.contains(COMPACT_IDX_BUCKET_COL));
      assert(idxTblColNames.contains(COMPACT_IDX_OFFSETS_ARRAY_COL));
      assert(idxTblColNames.size() == indexKeyNames.size() + 2);

      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if (!indexKeyNames.containsAll(selColRefNameList)) {
        LOG.info("Select list has non index key column : " +
            " Cannot use this index " + index.getIndexName());
        unusableIndexNames.add(index.getIndexName());
        continue;
      }

      // We need to check if all columns from index appear in select list only
      // in case of DISTINCT queries, In case group by queries, it is okay as long
      // as all columns from index appear in group-by-key list.
      if (getBoolVar(conf, RewriteVars.QUERY_HAS_DISTINCT)) {
        // Check if all columns from index are part of select list too
        if (!selColRefNameList.containsAll(indexKeyNames))  {
          LOG.info("Index has non select list columns " +
              " Cannot use index  " + index.getIndexName());
          unusableIndexNames.add(index.getIndexName());
          continue;
        }
      }

      //--------------------------------------------
      // Check if all columns in where predicate are part of index key columns
      // TODO: Currently we allow all predicates , would it be more efficient
      // (or at least not worse) to read from index_table and not from baseTable?
      if (!indexKeyNames.containsAll(predColRefs)) {
        LOG.info("Predicate column ref list has non index key column : " +
            " Cannot use this index  " + index.getIndexName());
        unusableIndexNames.add(index.getIndexName());
        continue;
      }

      if (!getBoolVar(conf, RewriteVars.QUERY_HAS_DISTINCT))  {
        //--------------------------------------------
        // For group by, we need to check if all keys are from index columns
        // itself. Here GB key order can be different than index columns but that does
        // not really matter for final result.
        // E.g. select c1, c2 from src group by c2, c1;
        // we can rewrite this one to:
        // select c1, c2 from src_cmpt_idx;
        if (!indexKeyNames.containsAll(gbKeyNameList)) {
          setBoolVar(conf, RewriteVars.GBY_KEY_HAS_NON_INDEX_COLS, true);
          return indexToKeysMap;
        }

        if (!gbKeyNameList.containsAll(indexKeyNames))  {
          // GB key and idx key are not same, don't remove GroupBy, but still do index scan
          removeGroupBy = false;
        }

        // This check prevents to remove GroupBy for cases where the GROUP BY key cols are
        // not simple expressions i.e. simple index key cols (in any order), but some
        // expressions on the the key cols.
        // e.g.
        // 1. GROUP BY key, f(key)
        //     FUTURE: If f(key) output is functionally dependent on key, then we should support
        //            it. However we don't have mechanism/info about f() yet to decide that.
        // 2. GROUP BY idxKey, 1
        //     FUTURE: GB Key has literals along with idxKeyCols. Develop a rewrite to eliminate the
        //            literals from GB key.
        // 3. GROUP BY idxKey, idxKey
        //     FUTURE: GB Key has dup idxKeyCols. Develop a rewrite to eliminate the dup key cols
        //            from GB key.
        if (getBoolVar(conf, RewriteVars.QUERY_HAS_GROUP_BY) &&
            indexKeyNames.size() < getIntVar(conf, RewriteVars.GBY_KEY_CNT)) {
          LOG.info("Group by key has only some non-indexed columns, GroupBy will be"
              + " preserved by rewrite optimization" );
          removeGroupBy = false;
        }

        // FUTURE: See if this can be relaxed.
        // If we have agg function (currently only COUNT is supported), check if its input are
        // from index. we currently support only that.
        if (colRefAggFuncInputList.size() > 0)  {
          for (int aggFuncIdx = 0; aggFuncIdx < colRefAggFuncInputList.size(); aggFuncIdx++)  {
            if (indexKeyNames.containsAll(colRefAggFuncInputList.get(aggFuncIdx)) == false) {
              LOG.info("Agg Func input is not present in index key columns. Currently " +
                  "only agg func on index columns are supported by rewrite optimization" );
              unusableIndexNames.add(index.getIndexName());
              continue;
            }

            // If we have count on some key, check if key is same as index key,
            if (colRefAggFuncInputList.get(aggFuncIdx).size() > 0)  {
              if (colRefAggFuncInputList.get(aggFuncIdx).containsAll(indexKeyNames))  {
                optimizeCount = true;
              }
            }
            else {
              optimizeCount = true;
            }
          }
        }
      }

      //Now that we are good to do this optimization, set parameters in context
      //which would be used by transformation procedure as inputs.

      //sub-query is needed only in case of optimizecount and complex gb keys?
      if(getBoolVar(conf, RewriteVars.QUERY_HAS_KEY_MANIP_FUNC) == false && !(optimizeCount == true && removeGroupBy == false) ) {
        setBoolVar(conf, RewriteVars.REMOVE_GROUP_BY, removeGroupBy);
        addTable(currentTableName, index.getIndexTableName());
      }else{
        setBoolVar(conf, RewriteVars.SHOULD_APPEND_SUBQUERY, true);
      }
      indexToKeysMap.put(indexTableName, indexKeyNames);
    }
    //If none of the index was found to be usable for rewrite
    if(unusableIndexNames.size() == indexTables.size()){
      LOG.info("No Valid Index Found to apply Optimization");
      return indexToKeysMap;
    }else{
      for (String unusableIndex : unusableIndexNames) {
        indexToKeysMap.remove(unusableIndex);
      }
    }
    return indexToKeysMap;

  }



}
