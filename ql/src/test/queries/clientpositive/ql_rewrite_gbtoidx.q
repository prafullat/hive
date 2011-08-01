
DROP TABLE lineitem;
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

CREATE INDEX lineitem_lshipdate_idx ON TABLE lineitem(l_shipdate) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(l_shipdate)");
ALTER INDEX lineitem_lshipdate_idx ON lineitem REBUILD;

set hive.optimize.index.groupby=true;

explain select l_shipdate, count(l_shipdate)
from lineitem
group by l_shipdate;

explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate);

explain select lastyear.month,
        thisyear.month,
        (thisyear.monthly_shipments - lastyear.monthly_shipments) /
lastyear.monthly_shipments as monthly_shipments_delta
   from (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1997
          group by year(l_shipdate), month(l_shipdate)
        )  lastyear join
        (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1998
          group by year(l_shipdate), month(l_shipdate)
        )  thisyear
  on lastyear.month = thisyear.month;

explain  select l_shipdate, cnt
from (select l_shipdate, count(l_shipdate) as cnt from lineitem group by l_shipdate
union all
select l_shipdate, l_orderkey as cnt
from lineitem) dummy;