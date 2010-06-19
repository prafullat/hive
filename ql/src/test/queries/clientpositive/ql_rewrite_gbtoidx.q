drop table src_cmpt_sum_idx;
create index src_cmpt_sum_idx type compact on table src(key) stored as textfile; 
explain select distinct key from src;
explain select key from src group by key;

set hive.ql.rw.gb_to_idx=true;
explain select distinct key from src;
explain select key from src group by key;
drop table src_cmpt_sum_idx;
