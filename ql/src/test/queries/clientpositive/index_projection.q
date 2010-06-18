EXPLAIN
CREATE INDEX srcpart_index_proj TYPE PROJECTION ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE PROJECTION ON TABLE srcpart(key);
UPDATE INDEX srcpart_index_proj;
SELECT x.* FROM srcpart_index_proj x WHERE x.ds = '2008-04-08' and x.hr = 11;
DROP TABLE srcpart_index_proj;

EXPLAIN
CREATE INDEX srcpart_index_proj TYPE PROJECTION ON TABLE srcpart(key) PARTITION(ds='2008-04-08', hr=11);
CREATE INDEX srcpart_index_proj TYPE PROJECTION ON TABLE srcpart(key) PARTITION(ds='2008-04-08', hr=11);
UPDATE INDEX srcpart_index_proj;
SELECT x.* FROM srcpart_index_proj x;
DROP TABLE srcpart_index_proj;