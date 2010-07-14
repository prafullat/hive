CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;

CREATE INDEX src_index TYPE COMPACT ON TABLE src_index_test_rc(key);
ALTER INDEX src_index ON src_index_test_rc REBUILD;
SELECT x.* FROM default__src_index_test_rc_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_index_test_rc_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

DROP INDEX src_index on src_index_test_rc;
DROP TABLE src_index_test_rc;
CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;

CREATE INDEX src_index TYPE COMPACT ON TABLE src_index_test_rc(key);
ALTER INDEX src_index ON src_index_test_rc REBUILD;
SELECT x.* FROM default__src_index_test_rc_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_index_test_rc_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

DROP INDEX src_index on src_index_test_rc;
DROP TABLE src_index_test_rc;
CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;

CREATE INDEX src_index TYPE COMPACT ON TABLE src_index_test_rc(key);
ALTER INDEX src_index ON src_index_test_rc REBUILD;
SELECT x.* FROM default__src_index_test_rc_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_index_test_rc_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

DROP INDEX src_index on src_index_test_rc;
DROP TABLE src_index_test_rc;
CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;

CREATE INDEX src_index TYPE COMPACT ON TABLE src_index_test_rc(key);
ALTER INDEX src_index ON src_index_test_rc REBUILD;
SELECT x.* FROM default__src_index_test_rc_src_index__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_index_test_rc_src_index__ WHERE key=100;
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100;

DROP INDEX src_index on src_index_test_rc;
DROP TABLE src_index_test_rc;