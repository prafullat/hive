DROP TABLE tbl;
CREATE TABLE tbl(key int, value string);

LOAD DATA LOCAL INPATH '/home/prajakta/hivelabs/src/hive/build/dist/examples/files/kv1.txt' OVERWRITE INTO TABLE tbl;



select key from tbl;