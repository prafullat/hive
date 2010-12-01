DROP TABLE tbl;
CREATE TABLE tbl(key int, value int);

EXPLAIN select key, count(key) from tbl where key = 1 group by key;
