DROP TABLE tbl;

DROP TABLE tbl_idx;

CREATE TABLE tbl(key int, value string);
CREATE TABLE tbl_idx(key int, value string);

LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input1.txt' OVERWRITE INTO TABLE tbl;
LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input2.txt' OVERWRITE INTO TABLE tbl_idx;

select key from tbl where key <50 order by key;
