EXPLAIN
<<<<<<< HEAD:ql/src/test/queries/clientpositive/index_compact_1.q
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
=======
CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key=100;
SET hive.index.compact.file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM src WHERE key=100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100 ORDER BY key;

DROP INDEX src_index on src;
>>>>>>> apache_master/trunk:ql/src/test/queries/clientpositive/index_compact_1.q
