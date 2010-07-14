EXPLAIN
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100;

DROP INDEX src_index on src;
EXPLAIN
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100;

DROP INDEX src_index on src;
EXPLAIN
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100;

DROP INDEX src_index on src;
EXPLAIN
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
CREATE INDEX src_index TYPE COMPACT ON TABLE src(key);
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100;

DROP INDEX src_index on src;