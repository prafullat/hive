create table src(key int, value int);
DROP INDEX src_cmpt_sum_idx on src;
DROP INDEX src1_cmpt_sum_idx on src;
CREATE INDEX src_cmpt_sum_idx TYPE COMPACT ON TABLE src(key) STORED AS textfile; 
CREATE INDEX src1_cmpt_sum_idx TYPE COMPACT ON TABLE src(key, value) STORED AS textfile; 
set hive.ql.rw.gb_to_idx=true;
EXPLAIN select key, count(key) from src where key = 1 group by key;
EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN select count(1) from src;
EXPLAIN select key, count(key) from src group by key;
EXPLAIN select count(key) from src;
EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT key FROM src GROUP BY key;

set hive.ql.rw.gb_to_idx=true;

EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT DISTINCT key, value FROM src;

EXPLAIN SELECT key FROM src GROUP BY key;
EXPLAIN SELECT key FROM src GROUP BY value, key;
EXPLAIN SELECT key, value FROM src GROUP BY value, key;

EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = 2;
EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = 2 AND key = 3;
EXPLAIN SELECT DISTINCT key, value FROM src WHERE value = key;


EXPLAIN SELECT key FROM src WHERE key = 3 GROUP BY key;
EXPLAIN SELECT key, value FROM src WHERE value = 1 GROUP BY key, value;

EXPLAIN SELECT * FROM (SELECT DISTINCT key, value FROM src) v1 WHERE v1.value = 2;

EXPLAIN SELECT key FROM src WHERE value = 2 GROUP BY key;
EXPLAIN SELECT DISTINCT key, substr(value,2,3) FROM src WHERE value = key;
EXPLAIN SELECT DISTINCT key, substr(value,2,3) FROM src;
EXPLAIN SELECT key FROM src GROUP BY key, substr(key,2,3);



DROP INDEX src_cmpt_sum_idx on src;
DROP INDEX src1_cmpt_sum_idx on src;
