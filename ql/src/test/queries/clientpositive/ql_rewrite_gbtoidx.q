DROP TABLE tbl;

DROP TABLE idx_tbl;

CREATE TABLE tbl(key int, value string);
CREATE TABLE idx_tbl(key int, value string);

LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input1.txt' OVERWRITE INTO TABLE tbl;
LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input2.txt' OVERWRITE INTO TABLE idx_tbl;


explain select key, count(key) from tbl group by key;