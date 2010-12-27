DROP TABLE tbl;

DROP TABLE idx_tbl;

CREATE TABLE tbl(key int, value string);
CREATE TABLE idx_tbl(key int, value string);

LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input1.txt' OVERWRITE INTO TABLE tbl;
LOAD DATA LOCAL INPATH '/home/pkalmegh/hivelabs/src/hive/input2.txt' OVERWRITE INTO TABLE idx_tbl;


CREATE INDEX tbl_key_idx ON TABLE tbl(key) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
ALTER INDEX tbl_key_idx ON tbl REBUILD;

CREATE INDEX idx_tbl_key_idx ON TABLE idx_tbl(key) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
ALTER INDEX idx_tbl_key_idx ON idx_tbl REBUILD;

SELECT count(*) FROM tbl group by value;