DROP TABLE tbl;
CREATE TABLE tbl(key int, value string);

LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/build/dist/examples/files/kv1.txt' OVERWRITE INTO TABLE tbl;


SELECT * FROM (SELECT DISTINCT key, value FROM tbl) v1 WHERE v1.value = 2;