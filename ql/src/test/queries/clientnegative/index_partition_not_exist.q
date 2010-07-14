CREATE INDEX srcpart_index_proj TYPE COMPACT ON TABLE srcpart(key);
ALTER INDEX srcpart_index_proj ON srcpart PARTITION(ds='2018-04-08', hr=12) REBUILD;
CREATE INDEX srcpart_index_proj TYPE SUMMARY ON TABLE srcpart(key) PARTITION(ds='2008-04-08', hr=11);
UPDATE INDEX srcpart_index_proj PARTITION(ds='2008-04-08', hr=12);