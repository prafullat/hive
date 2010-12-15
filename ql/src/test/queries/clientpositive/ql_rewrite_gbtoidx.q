DROP TABLE tbl;
CREATE TABLE tbl(key int, value int);
LOAD DATA LOCAL INPATH "/home/prafulla/projects/hive/./build/test/data/warehouse/src/kv1.txt" INTO TABLE tbl;
explain select key from tbl;
select key from tbl;
