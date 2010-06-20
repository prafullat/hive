DROP TABLE src_cmpt_sum_idx;
DROP TABLE src_proj_idx;
DROP TABLE src1_cmpt_sum_idx;
CREATE INDEX src_proj_idx TYPE PROJECTION ON TABLE src(key) STORED AS textfile;
CREATE INDEX src_cmpt_sum_idx TYPE COMPACT ON TABLE src(key) STORED AS textfile; 
CREATE INDEX src1_cmpt_sum_idx TYPE COMPACT ON TABLE src(key, value) STORED AS textfile; 

EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT key FROM src GROUP BY key;

set hive.ql.rw.gb_to_idx=true;
EXPLAIN SELECT DISTINCT key FROM src;
EXPLAIN SELECT key FROM src GROUP BY key;
EXPLAIN SELECT key FROM src GROUP BY value, key;
EXPLAIN SELECT key, value FROM src GROUP BY value, key;
DROP TABLE src_cmpt_sum_idx;
DROP TABLE src_proj_idx;
DROP TABLE src1_cmpt_sum_idx;

