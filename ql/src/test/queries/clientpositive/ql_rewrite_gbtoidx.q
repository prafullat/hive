DROP TABLE tbl;
CREATE TABLE tbl(key int, value string);

LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/build/dist/examples/files/kv1.txt' OVERWRITE INTO TABLE tbl;

explain extended SELECT key FROM tbl group by key;
SELECT key FROM tbl group by key;
