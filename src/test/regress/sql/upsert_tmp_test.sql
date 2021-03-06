\c upsert
CREATE SCHEMA upsert_test_tmp;
SET CURRENT_SCHEMA TO upsert_test_tmp;
CREATE TYPE atype AS(a int, b int);
CREATE TYPE btype AS(a int, b atype, c varchar[3]);
CREATE TEMP TABLE t_hash_tmp_0 (c1 INT, c2 INT, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype);
CREATE TEMP TABLE t_hash_tmp_1 (c1 INT, c2 INT PRIMARY KEY, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype);
CREATE TEMP TABLE t_rep_tmp_0 (c1 INT, c2 INT, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype) ;
CREATE TEMP TABLE t_rep_tmp_1 (c1 INT, c2 INT PRIMARY KEY, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype) ;

-- hash table
-- tmp table without primary key
INSERT INTO t_hash_tmp_0 VALUES(1, 1, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(1, 2, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_hash_tmp_0 VALUES(2, 3, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(2, 4, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE c2 = 100;

-- tmp table with primary key
-- multi insert
INSERT INTO t_hash_tmp_1 VALUES(1, 1, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(1, 2, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_hash_tmp_1 VALUES(2, 3, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(2, 4, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_hash_tmp_1 ORDER BY c2;

INSERT INTO t_hash_tmp_1 VALUES(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 2, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_hash_tmp_1 VALUES(20, 3, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(21, 4, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_hash_tmp_1 ORDER BY c2;

-- insert and update same tuple, and update twice for another same tuple
INSERT INTO t_hash_tmp_1 VALUES(0, 5, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(1, 5, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}')),
	(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 1, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE NOTHING;
SELECT * FROM t_hash_tmp_1 ORDER BY c2;

INSERT INTO t_hash_tmp_1 VALUES(0, 5, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(1, 5, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}')),
	(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 1, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_hash_tmp_1 ORDER BY c2;

-- replication table
-- tmp table without primary key
INSERT INTO t_rep_tmp_0 VALUES(1, 1, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(1, 2, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_rep_tmp_0 VALUES(2, 3, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(2, 4, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE c2 = 100;

-- tmp table with primary key
-- multi insert
INSERT INTO t_rep_tmp_1 VALUES(1, 1, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(1, 2, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_rep_tmp_1 VALUES(2, 3, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}')),
	(2, 4, 'C3', '{1,2,3}', '{10,20,30,40,50}', ROW(10,20), ROW(100, ROW(10,20), '{100,200}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_rep_tmp_1 ORDER BY c2;

INSERT INTO t_rep_tmp_1 VALUES(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 2, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE NOTHING;
INSERT INTO t_rep_tmp_1 VALUES(20, 3, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(21, 4, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_rep_tmp_1 ORDER BY c2;

-- insert and update same tuple, and update twice for another same tuple
INSERT INTO t_rep_tmp_1 VALUES(0, 5, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(1, 5, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}')),
	(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 1, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE NOTHING;
SELECT * FROM t_rep_tmp_1 ORDER BY c2;

INSERT INTO t_rep_tmp_1 VALUES(0, 5, 'C30', '{10,20,30}', '{10,20,30,40,50}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(1, 5, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}')),
	(10, 1, 'C30', '{10,20,30}', '{100,200,300,400,500}', ROW(100,200), ROW(1000, ROW(100,200), '{1000,2000}')),
	(11, 1, 'C31', '{11,21,31}', '{101,201,301,401,501}', ROW(101,201), ROW(1001, ROW(101,201), '{1001,2001}'))
	ON DUPLICATE KEY UPDATE c1 = EXCLUDED.c1, c3 = EXCLUDED.c3, c4 = EXCLUDED.c4, c5 = EXCLUDED.c5, c6 = EXCLUDED.c6, c7 = EXCLUDED.c7;
SELECT * FROM t_rep_tmp_1 ORDER BY c2;

DROP SCHEMA upsert_test_tmp CASCADE;
