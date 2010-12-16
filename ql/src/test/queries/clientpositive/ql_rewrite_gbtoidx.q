DROP TABLE tbl;
CREATE TABLE tbl(key int, value string);

LOAD DATA LOCAL INPATH '/tmp/kv1.txt' OVERWRITE INTO TABLE tbl;


explain select key from tbl where key <50 
select key from tbl where key <50;
