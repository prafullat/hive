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