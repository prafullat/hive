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

CREATE INDEX lineitem_lshipdate_idx ON TABLE lineitem(l_shipdate) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
ALTER INDEX lineitem_lshipdate_idx ON lineitem REBUILD;


select l_shipdate
from lineitem
group by l_shipdate;


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











explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        sum(sz)
from (
select l_shipdate, size(`_offsets`) as sz
from default__lineitem_lineitem_lshipdate_idx__
) t
group by year(l_shipdate), month(l_shipdate);


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


DROP TABLE tbl;
CREATE TABLE tbl(key int, value int);

EXPLAIN select key, count(key) from tbl where key = 1 group by key;
