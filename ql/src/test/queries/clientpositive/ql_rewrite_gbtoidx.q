DROP TABLE tbl;
CREATE TABLE tbl(key int, value int);
CREATE INDEX lineitem_lshipdate_idx ON TABLE tbl(key) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
explain select key from tbl;
select key from tbl;
