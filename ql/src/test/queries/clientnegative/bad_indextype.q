<<<<<<< HEAD:ql/src/test/queries/clientnegative/bad_indextype.q
CREATE INDEX srcpart_index_proj TYPE UNKNOWN ON TABLE srcpart(key);
CREATE INDEX srcpart_index_proj TYPE UNKNOWN ON TABLE srcpart(key);
=======
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) AS 'UNKNOWN' WITH DEFERRED REBUILD;
>>>>>>> apache_master/trunk:ql/src/test/queries/clientnegative/bad_indextype.q
