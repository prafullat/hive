package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;

public class RewriteCanApplyCtx implements NodeProcessorCtx {

  protected final  Log LOG = LogFactory.getLog(RewriteCanApplyCtx.class.getName());

  public RewriteCanApplyCtx() {
    baseToIdxTableMap = new HashMap<String, String>();
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

  /***************************************Index Validation Variables***************************************/
   final String SUPPORTED_INDEX_TYPE =
    "org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler";
   final String COMPACT_IDX_BUCKET_COL = "_bucketname";
   final String COMPACT_IDX_OFFSETS_ARRAY_COL = "_offsets";
   final Set<String> selColRefNameList = new LinkedHashSet<String>();
   final List<String> predColRefs = new ArrayList<String>();
   final List<String> gbKeyNameList = new ArrayList<String>();
   final List<List<String>> colRefAggFuncInputList = new ArrayList<List<String>>();
   HiveConf conf = null;
   private String indexTableName = "";
   int aggFuncCnt = 0;


   public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public String getIndexTableName() {
    return indexTableName;
  }


   //Map for base table to index table mapping
   //TableScan operator for base table will be modified to read from index table
   private final HashMap<String, String> baseToIdxTableMap;

   public void addTable(String baseTableName, String indexTableName) {
     baseToIdxTableMap.put(baseTableName, indexTableName);
   }

   public String findBaseTable(String baseTableName)  {
     return baseToIdxTableMap.get(baseTableName);
   }


  public void setIndexName(String indexName) {
    this.indexTableName = indexName;
  }

  public Set<String> getIndexKeyNames() {
    return indexKeyNames;
  }

  public void setIndexKeyNames(Set<String> indexKeyNames) {
    this.indexKeyNames = indexKeyNames;
  }

  private Set<String> indexKeyNames = new LinkedHashSet<String>();

  private  ParseContext parseContext = null;
  private Hive hiveDb;
  public Hive getHiveDb() {
    return hiveDb;
  }

  public void setHiveDb(Hive hiveDb) {
    this.hiveDb = hiveDb;
  }

  private String currentTableName = null;


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

  private  String getName() {
    return "RewriteGBUsingIndex";
  }


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



  boolean isIndexUsable(List<Index> indexTables){
    Index index = null;
    Hive hiveInstance = hiveDb;

    // This code block iterates over indexes on the table and picks up the
    // first index that satisfies the rewrite criteria.
    ArrayList<String> unusableIndexNames = new ArrayList<String>();
    for (int idxCtr = 0; idxCtr < indexTables.size(); idxCtr++)  {
      boolean removeGroupBy = true;
      boolean optimizeCount = false;
      List<String> idxKeyColsNames = new ArrayList<String>();

      index = indexTables.get(idxCtr);
      indexTableName = index.getIndexTableName();
      //Getting index key columns
      StorageDescriptor sd = index.getSd();
      List<FieldSchema> idxColList = sd.getCols();
      for (FieldSchema fieldSchema : idxColList) {
        idxKeyColsNames.add(fieldSchema.getName());
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
        return false;
      }
      assert(idxTblColNames.contains(COMPACT_IDX_BUCKET_COL));
      assert(idxTblColNames.contains(COMPACT_IDX_OFFSETS_ARRAY_COL));
      assert(idxTblColNames.size() == idxKeyColsNames.size() + 2);

      //--------------------------------------------
      //Check if all columns in select list are part of index key columns
      if (!idxKeyColsNames.containsAll(selColRefNameList)) {
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
        if (!selColRefNameList.containsAll(idxKeyColsNames))  {
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
      if (!idxKeyColsNames.containsAll(predColRefs)) {
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
        if (!idxKeyColsNames.containsAll(gbKeyNameList)) {
          setBoolVar(conf, RewriteVars.GBY_KEY_HAS_NON_INDEX_COLS, true);
          return false;
        }

        if (!gbKeyNameList.containsAll(idxKeyColsNames))  {
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
            idxKeyColsNames.size() < getIntVar(conf, RewriteVars.GBY_KEY_CNT)) {
          LOG.info("Group by key has only some non-indexed columns, GroupBy will be"
              + " preserved by rewrite " + getName() + " optimization" );
          removeGroupBy = false;
        }

        // FUTURE: See if this can be relaxed.
        // If we have agg function (currently only COUNT is supported), check if its input are
        // from index. we currently support only that.
        if (colRefAggFuncInputList.size() > 0)  {
          for (int aggFuncIdx = 0; aggFuncIdx < colRefAggFuncInputList.size(); aggFuncIdx++)  {
            if (idxKeyColsNames.containsAll(colRefAggFuncInputList.get(aggFuncIdx)) == false) {
              LOG.info("Agg Func input is not present in index key columns. Currently " +
                  "only agg func on index columns are supported by rewrite " + getName() + " optimization" );
              unusableIndexNames.add(index.getIndexName());
              continue;
            }

            // If we have count on some key, check if key is same as index key,
            if (colRefAggFuncInputList.get(aggFuncIdx).size() > 0)  {
              if (colRefAggFuncInputList.get(aggFuncIdx).containsAll(idxKeyColsNames))  {
                optimizeCount = true;
              }
            }
            else  {
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

    }

    if(unusableIndexNames.size() == indexTables.size()){
      LOG.info("No Valid Index Found to apply Optimization");
      return false;
    }

    return true;

  }



  boolean checkIfOptimizationCanApply(){
    if (getBoolVar(conf, RewriteVars.QUERY_HAS_MULTIPLE_TABLES)) {
      LOG.info("Query has more than one table " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }//1
    if (getIntVar(conf, RewriteVars.NO_OF_SUBQUERIES) != 0) {
      LOG.info("Query has more than one subqueries " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }//2
    if (getBoolVar(conf, RewriteVars.TABLE_HAS_NO_INDEX)) {
      LOG.info("Table " + currentTableName + " does not have compact index. " +
          "Cannot apply " + getName() + " optimization" );
      return false;
    }//3
    if(getBoolVar(conf, RewriteVars.QUERY_HAS_DISTRIBUTE_BY)){
      LOG.info("Query has distributeby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }//4
    if(getBoolVar(conf, RewriteVars.QUERY_HAS_SORT_BY)){
      LOG.info("Query has sortby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }//5
    if(getBoolVar(conf, RewriteVars.QUERY_HAS_ORDER_BY)){
      LOG.info("Query has orderby clause, " +
          "that is not supported with " + getName() + " optimization" );
      return false;
    }//6
    if(getIntVar(conf, RewriteVars.AGG_FUNC_CNT) > 1 ){
      LOG.info("More than 1 agg funcs: " +
          "Not supported by " + getName() + " optimization" );
      return false;
    }//7
    if(getBoolVar(conf, RewriteVars.AGG_FUNC_IS_NOT_COUNT)){
      LOG.info("Agg func other than count is " +
          "not supported by " + getName() + " optimization" );
      return false;
    }//8
    if(getBoolVar(conf, RewriteVars.AGG_FUNC_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs " +
          "of agg func, skipping " + getName() + " optimization" );
      return false;
    }//9
    if(getBoolVar(conf, RewriteVars.WHR_CLAUSE_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for where clause, "
          + "skipping " + getName() + " optimization" );
      return false;
    }//10
/*      if(getBoolVar(conf, RewriteVars.QUERY_HAS_DISTINCT)){
      LOG.info("Select-list has distinct. " +
          "Cannot apply the rewrite " + getName() + " optimization" );
      return false;
    }//11
*/      if(getBoolVar(conf, RewriteVars.SEL_HAS_NON_COL_REF)){
      LOG.info("Select-list has some non-col-ref expression. " +
          "Cannot apply the rewrite " + getName() + " optimization" );
      return false;
    }//12
    if(getBoolVar(conf, RewriteVars.SEL_CLAUSE_COLS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for select list, "
          + "skipping " + getName() + " optimization" );
      return false;
    }//13
    if(getBoolVar(conf, RewriteVars.GBY_KEYS_FETCH_EXCEPTION)){
      LOG.info("Got exception while locating child col refs for GroupBy key, "
          + "skipping " + getName() + " optimization" );
      return false;
    }//14
    if(getBoolVar(conf, RewriteVars.GBY_NOT_ON_COUNT_KEYS)){
      LOG.info("Currently count function needs group by on key columns, "
          + "Cannot apply this " + getName() + " optimization" );
      return false;
    }//15
    if(getBoolVar(conf, RewriteVars.IDX_TBL_SEARCH_EXCEPTION)){
      LOG.info("Got exception while locating index table, " +
          "skipping " + getName() + " optimization" );
      return false;
    }//16
    if(getBoolVar(conf, RewriteVars.GBY_KEY_HAS_NON_INDEX_COLS)){
      LOG.info("Group by key has some non-indexed columns, " +
          "Cannot apply rewrite " + getName() + " optimization" );
      return false;
    }//17
    return true;
  }




}
