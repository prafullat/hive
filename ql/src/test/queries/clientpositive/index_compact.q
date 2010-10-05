<<<<<<< HEAD:ql/src/test/queries/clientpositive/index_compact.q
EXPLAIN
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
ALTER INDEX srcpart_index_proj ON srcpart REBUILD;
SELECT x.* FROM default__srcpart_srcpart_index_proj__ x WHERE x.ds = '2008-04-08' and x.hr = 11;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08';
SET hive.exec.index_file=/tmp/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08';

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08' and x.hr = 11;
SET hive.exec.index_file=/tmp/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11;
=======
DROP INDEX srcpart_index_proj on srcpart;

EXPLAIN
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX srcpart_index_proj ON srcpart REBUILD;
SELECT x.* FROM default__srcpart_srcpart_index_proj__ x WHERE x.ds = '2008-04-08' and x.hr = 11 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08';
SET hive.index.compact.file=/tmp/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08' and x.hr = 11;
SET hive.index.compact.file=/tmp/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11 ORDER BY key;
>>>>>>> apache_master/trunk:ql/src/test/queries/clientpositive/index_compact.q

DROP INDEX srcpart_index_proj on srcpart;

EXPLAIN
<<<<<<< HEAD:ql/src/test/queries/clientpositive/index_compact.q
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
=======
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
>>>>>>> apache_master/trunk:ql/src/test/queries/clientpositive/index_compact.q
ALTER  INDEX srcpart_index_proj ON srcpart REBUILD;
SELECT x.* FROM default__srcpart_srcpart_index_proj__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "/tmp/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ WHERE key=100;
<<<<<<< HEAD:ql/src/test/queries/clientpositive/index_compact.q
SET hive.exec.index_file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.io.HiveIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100;

DROP INDEX srcpart_index_proj on srcpart;
EXPLAIN
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
UPDATE INDEX srcpart_index_proj;
SELECT x.* FROM srcpart_index_proj x WHERE x.ds = '2008-04-08' and x.hr = 11;
DROP TABLE srcpart_index_proj;

EXPLAIN
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
UPDATE INDEX srcpart_index_proj;
SELECT x.* FROM srcpart_index_proj x;
DROP TABLE srcpart_index_proj;
=======
SET hive.index.compact.file=/tmp/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100 ORDER BY key;

DROP INDEX srcpart_index_proj on srcpart;
>>>>>>> apache_master/trunk:ql/src/test/queries/clientpositive/index_compact.q
