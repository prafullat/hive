drop table t;
create table t(i int, j int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INPATH '/home/prafulla/projects/hive-data/t1_data' OVERWRITE INTO TABLE t;
select * from t where i = 1;
prafulla

DROP TABLE lineitem;
drop index lineitem_lshipdate_idx on lineitem;
CREATE TABLE lineitem (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

CREATE INDEX lineitem_lshipdate_idx TYPE COMPACT ON TABLE lineitem(l_shipdate) STORED AS textfile; 


set hive.ql.rw.gb_to_idx=true;

explain select l_shipdate,
       count(1)
from 
lineitem
group by l_shipdate;


explain select lastyear.month,
        thisyear.month,
        (thisyear.monthly_shipments - lastyear.monthly_shipments) /
lastyear.monthly_shipments as monthly_shipments_delta
   from (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(1) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1997
          group by year(l_shipdate), month(l_shipdate)
        )  lastyear join
        (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(1) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1998
          group by year(l_shipdate), month(l_shipdate)
        )  thisyear
  on lastyear.month = thisyear.month;



explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(1) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate);



explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        sum(sz)
from (
select l_shipdate, size(`_offsets`) as sz
from default__lineitem_lineitem_lshipdate_idx__
) t
group by year(l_shipdate), month(l_shipdate);


explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(1) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate);











exp select year(l_shipdate) as year,
        month(l_shipdate) as month,
        sum(sz)
from (
select l_shipdate, size(`_offsets`) as sz
from default__lineitem_lineitem_lshipdate_idx__
) t
group by year(l_shipdate), month(l_shipdate);




select year(l_shipdate) as year,
        month(l_shipdate) as month,
        sum(sz)
from (
select l_shipdate, size(`_offsets`) as sz
from default__tpch10g_lineitem_tpch10g_lineitem_shipdate_idx__
) t
group by year(l_shipdate), month(l_shipdate);
Time taken: 36.005 seconds


explain select year(L_SHIPDATE), month(L_SHIPDATE) as month_bkt, COUNT(1)
  from lineitem
group by year(L_SHIPDATE), month(L_SHIPDATE);


explain select lastyear.month,
        thisyear.month,
        (thisyear.monthly_shipments - lastyear.monthly_shipments) /
lastyear.monthly_shipments as monthly_shipments_delta
   from (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(1) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1997
          group by year(l_shipdate), month(l_shipdate)
        )  lastyear join
        (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(1) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1998
          group by year(l_shipdate), month(l_shipdate)
        )  thisyear
  on lastyear.month = thisyear.month
    and lastyear.year = thisyear.year;



create table src(key int, value int);
DROP INDEX src_cmpt_sum_idx on src;
DROP INDEX src1_cmpt_sum_idx on src;
CREATE INDEX src_cmpt_sum_idx TYPE COMPACT ON TABLE src(key) STORED AS textfile; 
CREATE INDEX src1_cmpt_sum_idx TYPE COMPACT ON TABLE src(key, value) STORED AS textfile; 
EXPLAIN select key, count(key) from src where key = 1 group by key;
EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN select count(1) from src;
EXPLAIN select key, count(key) from src group by key;
EXPLAIN select count(key) from src;
EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT key FROM src GROUP BY key;

set hive.ql.rw.gb_to_idx=true;

EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT DISTINCT key, value FROM src;

EXPLAIN SELECT key FROM src GROUP BY key;
EXPLAIN SELECT key FROM src GROUP BY value, key;
EXPLAIN SELECT key, value FROM src GROUP BY value, key;

EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = 2;
EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = 2 AND key = 3;
EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = key;


EXPLAIN SELECT key FROM src WHERE key = 3 GROUP BY key;
EXPLAIN SELECT key, value FROM src WHERE value = 1 GROUP BY key, value;

EXPLAIN SELECT * FROM (SELECT DISTINCT key, value FROM src) v1 WHERE v1.value = 2;

EXPLAIN SELECT key FROM src WHERE value = 2 GROUP BY key;
EXPLAIN SELECT DISTINCT key, substr(value,2,3) FROM src WHERE value = key;
EXPLAIN SELECT DISTINCT key, substr(value,2,3) FROM src;
EXPLAIN SELECT key FROM src GROUP BY key, substr(key,2,3);



DROP INDEX src_cmpt_sum_idx on src;
DROP INDEX src1_cmpt_sum_idx on src;
