--
-- Test inheritance features
--
CREATE TABLE a (aa TEXT) distribute by roundrobin;
CREATE TABLE b (bb TEXT) INHERITS (a) distribute by roundrobin;
CREATE TABLE c (cc TEXT) INHERITS (a) distribute by roundrobin;
CREATE TABLE d (dd TEXT) INHERITS (b,c,a) distribute by roundrobin;

INSERT INTO a(aa) VALUES('aaa');
INSERT INTO a(aa) VALUES('aaaa');
INSERT INTO a(aa) VALUES('aaaaa');
INSERT INTO a(aa) VALUES('aaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaaa');

INSERT INTO b(aa) VALUES('bbb');
INSERT INTO b(aa) VALUES('bbbb');
INSERT INTO b(aa) VALUES('bbbbb');
INSERT INTO b(aa) VALUES('bbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbbb');

INSERT INTO c(aa) VALUES('ccc');
INSERT INTO c(aa) VALUES('cccc');
INSERT INTO c(aa) VALUES('ccccc');
INSERT INTO c(aa) VALUES('cccccc');
INSERT INTO c(aa) VALUES('ccccccc');
INSERT INTO c(aa) VALUES('cccccccc');

INSERT INTO d(aa) VALUES('ddd');
INSERT INTO d(aa) VALUES('dddd');
INSERT INTO d(aa) VALUES('ddddd');
INSERT INTO d(aa) VALUES('dddddd');
INSERT INTO d(aa) VALUES('ddddddd');
INSERT INTO d(aa) VALUES('dddddddd');

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM a ORDER BY a.aa;
SELECT * from b ORDER BY b.aa;
SELECT * FROM c ORDER BY c.aa;
SELECT * from d ORDER BY d.aa;
SELECT * FROM ONLY a ORDER BY a.aa;
SELECT * from ONLY b ORDER BY b.aa;
SELECT * FROM ONLY c ORDER BY c.aa;
SELECT * from ONLY d ORDER BY d.aa;

UPDATE a SET aa='zzzz' WHERE aa='aaaa';
UPDATE ONLY a SET aa='zzzzz' WHERE aa='aaaaa';
UPDATE b SET aa='zzz' WHERE aa='aaa';
UPDATE ONLY b SET aa='zzz' WHERE aa='aaa';
UPDATE a SET aa='zzzzzz' WHERE aa LIKE 'aaa%';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM a ORDER BY a.aa;
SELECT * from b ORDER BY b.aa;
SELECT * FROM c ORDER BY c.aa;
SELECT * from d ORDER BY d.aa;
SELECT * FROM ONLY a ORDER BY a.aa;
SELECT * from ONLY b ORDER BY b.aa;
SELECT * FROM ONLY c ORDER BY c.aa;
SELECT * from ONLY d ORDER BY d.aa;

UPDATE b SET aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM a ORDER BY a.aa;
SELECT * from b ORDER BY b.aa;
SELECT * FROM c ORDER BY c.aa;
SELECT * from d ORDER BY d.aa;
SELECT * FROM ONLY a ORDER BY a.aa;
SELECT * from ONLY b ORDER BY b.aa;
SELECT * FROM ONLY c ORDER BY c.aa;
SELECT * from ONLY d ORDER BY d.aa;

UPDATE a SET aa='new';

DELETE FROM ONLY c WHERE aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM a ORDER BY a.aa;
SELECT * from b ORDER BY b.aa;
SELECT * FROM c ORDER BY c.aa;
SELECT * from d ORDER BY d.aa;
SELECT * FROM ONLY a ORDER BY a.aa;
SELECT * from ONLY b ORDER BY b.aa;
SELECT * FROM ONLY c ORDER BY c.aa;
SELECT * from ONLY d ORDER BY d.aa;

DELETE FROM a;

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid ORDER BY relname, a.aa;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid ORDER BY relname, b.aa;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid ORDER BY relname, c.aa;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid ORDER BY relname, d.aa;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM a ORDER BY a.aa;
SELECT * from b ORDER BY b.aa;
SELECT * FROM c ORDER BY c.aa;
SELECT * from d ORDER BY d.aa;
SELECT * FROM ONLY a ORDER BY a.aa;
SELECT * from ONLY b ORDER BY b.aa;
SELECT * FROM ONLY c ORDER BY c.aa;
SELECT * from ONLY d ORDER BY d.aa;

-- Enforce use of COMMIT instead of 2PC for temporary objects

-- Confirm PRIMARY KEY adds NOT NULL constraint to child table
CREATE TEMP TABLE z (b TEXT, PRIMARY KEY(aa, b)) inherits (a);
INSERT INTO z VALUES (NULL, 'text'); -- should fail
DROP TABLE z;

-- Check UPDATE with inherited target and an inherited source table
create temp table foo(f1 int, f2 int);
create temp table foo2(f3 int) inherits (foo);
create temp table bar(f1 int, f2 int);
create temp table bar2(f3 int) inherits (bar);

insert into foo values(1,1);
insert into foo values(3,3);
insert into foo2 values(2,2,2);
insert into foo2 values(3,3,3);
insert into bar values(1,1);
insert into bar values(2,2);
insert into bar values(3,3);
insert into bar values(4,4);
insert into bar2 values(1,1,1);
insert into bar2 values(2,2,2);
insert into bar2 values(3,3,3);
insert into bar2 values(4,4,4);

update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

SELECT relname, bar.* FROM bar, pg_class where bar.tableoid = pg_class.oid
order by 1,2;
-- In Postgres-XC OIDs are not consistent across the cluster. Hence above
-- queries do not show any result. Hence in order to ensure data consistency, we
-- add following SQLs. In case above set of queries start producing valid
-- results in XC, we should remove the following set
SELECT * FROM bar ORDER BY f1, f2;
SELECT * FROM ONLY bar ORDER BY f1, f2;
SELECT * FROM bar2 ORDER BY f1, f2;

/* Test multiple inheritance of column defaults */

CREATE TABLE firstparent (tomorrow date default now()::date + 1);
CREATE TABLE secondparent (tomorrow date default  now() :: date  +  1);
CREATE TABLE jointchild () INHERITS (firstparent, secondparent);  -- ok
CREATE TABLE thirdparent (tomorrow date default now()::date - 1);
CREATE TABLE otherchild () INHERITS (firstparent, thirdparent);  -- not ok
CREATE TABLE otherchild (tomorrow date default now())
  INHERITS (firstparent, thirdparent);  -- ok, child resolves ambiguous default

DROP TABLE firstparent, secondparent, jointchild, thirdparent, otherchild;

-- Test changing the type of inherited columns
insert into d values('test','one','two','three');
alter table a alter column aa type integer using bit_length(aa);
select * from d;

-- Test non-inheritable parent constraints
create table p1(ff1 int);
alter table p1 add constraint p1chk check (ff1 > 0) no inherit;
alter table p1 add constraint p2chk check (ff1 > 10);
-- connoinherit should be true for NO INHERIT constraint
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.connoinherit from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname = 'p1' order by 1,2;

-- Test that child does not inherit NO INHERIT constraints
create table c1 () inherits (p1);
\d p1
\d c1

drop table p1 cascade;

-- Tests for casting between the rowtypes of parent and child
-- tables. See the pgsql-hackers thread beginning Dec. 4/04
create table base (i integer);
create table derived () inherits (base);
insert into derived (i) values (0);
select derived::base from derived;
drop table derived;
drop table base;

create table p1(ff1 int);
create table p2(f1 text);
create function p2text(p2) returns text as 'select $1.f1' language sql;
create table c1(f3 int) inherits(p1,p2);
insert into c1 values(123456789, 'hi', 42);
select p2text(c1.*) from c1;
drop function p2text(p2);
drop table c1;
drop table p2;
drop table p1;

CREATE TABLE ac (aa TEXT);
alter table ac add constraint ac_check check (aa is not null);
CREATE TABLE bc (bb TEXT) INHERITS (ac);
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);

alter table bc drop constraint ac_check;  -- fail, disallowed
alter table ac drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

-- try the unnamed-constraint case
alter table ac add check (aa is not null);
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);

alter table bc drop constraint ac_aa_check;  -- fail, disallowed
alter table ac drop constraint ac_aa_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

alter table ac add constraint ac_check check (aa is not null);
alter table bc no inherit ac;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
alter table bc drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
alter table ac drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

drop table bc;
drop table ac;

create table ac (a int constraint check_a check (a <> 0));
create table bc (a int constraint check_a check (a <> 0), b int constraint check_b check (b <> 0)) inherits (ac);
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

drop table bc;
drop table ac;

create table ac (a int constraint check_a check (a <> 0));
create table bc (b int constraint check_b check (b <> 0));
create table cc (c int constraint check_c check (c <> 0)) inherits (ac, bc);
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;

alter table cc no inherit bc;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;

drop table cc;
drop table bc;
drop table ac;

create table p1(f1 int);
create table p2(f2 int);
create table c1(f3 int) inherits(p1,p2);
insert into c1 values(1,-1,2);
alter table p2 add constraint cc check (f2>0);  -- fail
alter table p2 add check (f2>0);  -- check it without a name, too
delete from c1;
insert into c1 values(1,1,2);
alter table p2 add check (f2>0);
insert into c1 values(1,-1,2);  -- fail
create table c2(f3 int) inherits(p1,p2);
\d c2
create table c3 (f4 int) inherits(c1,c2);
\d c3
drop table p1 cascade;
drop table p2 cascade;

create table pp1 (f1 int);
create table cc1 (f2 text, f3 int) inherits (pp1);
alter table pp1 add column a1 int check (a1 > 0);
\d cc1
create table cc2(f4 float) inherits(pp1,cc1);
\d cc2
alter table pp1 add column a2 int check (a2 > 0);
\d cc2
drop table pp1 cascade;

-- Test for renaming in simple multiple inheritance
CREATE TABLE inht1 (a int, b int);
CREATE TABLE inhs1 (b int, c int);
CREATE TABLE inhts (d int) INHERITS (inht1, inhs1);

ALTER TABLE inht1 RENAME a TO aa;
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
ALTER TABLE inhts RENAME aa TO aaa;      -- to be failed
ALTER TABLE inhts RENAME d TO dd;
\d+ inhts

DROP TABLE inhts;

-- Test for renaming in diamond inheritance
CREATE TABLE inht2 (x int) INHERITS (inht1);
CREATE TABLE inht3 (y int) INHERITS (inht1);
CREATE TABLE inht4 (z int) INHERITS (inht2, inht3);

ALTER TABLE inht1 RENAME aa TO aaa;
\d+ inht4

CREATE TABLE inhts (d int) INHERITS (inht2, inhs1);
ALTER TABLE inht1 RENAME aaa TO aaaa;
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
\d+ inhts

WITH RECURSIVE r AS (
  SELECT 'inht1'::regclass AS inhrelid
UNION ALL
  SELECT c.inhrelid FROM pg_inherits c, r WHERE r.inhrelid = c.inhparent
)
SELECT a.attrelid::regclass, a.attname, a.attinhcount, e.expected
  FROM (SELECT inhrelid, count(*) AS expected FROM pg_inherits
        WHERE inhparent IN (SELECT inhrelid FROM r) GROUP BY inhrelid) e
  JOIN pg_attribute a ON e.inhrelid = a.attrelid WHERE NOT attislocal
  ORDER BY a.attrelid::regclass::name, a.attnum;

DROP TABLE inht1, inhs1 CASCADE;


-- Test non-inheritable indices [UNIQUE, EXCLUDE] contraints
CREATE TABLE test_constraints (id int, val1 varchar, val2 int, UNIQUE(val1, val2));
CREATE TABLE test_constraints_inh () INHERITS (test_constraints);
\d+ test_constraints
ALTER TABLE ONLY test_constraints DROP CONSTRAINT test_constraints_val1_val2_key;
\d+ test_constraints
\d+ test_constraints_inh
DROP TABLE test_constraints_inh;
DROP TABLE test_constraints;

CREATE TABLE test_ex_constraints (
    c circle,
    EXCLUDE USING gist (c WITH &&)
) DISTRIBUTE BY REPLICATION;
CREATE TABLE test_ex_constraints_inh () INHERITS (test_ex_constraints);
\d+ test_ex_constraints
ALTER TABLE test_ex_constraints DROP CONSTRAINT test_ex_constraints_c_excl;
\d+ test_ex_constraints
\d+ test_ex_constraints_inh
DROP TABLE test_ex_constraints_inh;
DROP TABLE test_ex_constraints;

-- Test non-inheritable foreign key contraints
CREATE TABLE test_primary_constraints(id int PRIMARY KEY);
CREATE TABLE test_foreign_constraints(id1 int REFERENCES test_primary_constraints(id));
CREATE TABLE test_foreign_constraints_inh () INHERITS (test_foreign_constraints);
\d+ test_primary_constraints
\d+ test_foreign_constraints
ALTER TABLE test_foreign_constraints DROP CONSTRAINT test_foreign_constraints_id1_fkey;
\d+ test_foreign_constraints
\d+ test_foreign_constraints_inh
DROP TABLE test_foreign_constraints_inh;
DROP TABLE test_foreign_constraints;
DROP TABLE test_primary_constraints;

--
-- Test parameterized append plans for inheritance trees
--

create table patest0 (id, x) as
  select x, x from generate_series(0,1000) x;
create table patest1() inherits (patest0);
insert into patest1
  select x, x from generate_series(0,1000) x;
create table patest2() inherits (patest0);
insert into patest2
  select x, x from generate_series(0,1000) x;
create index patest0i on patest0(id);
create index patest1i on patest1(id);
create index patest2i on patest2(id);
analyze patest0;
analyze patest1;
analyze patest2;

explain (costs off)
select * from patest0 join (select f1 from int4_tbl where f1 >= 0 order by f1 limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl where f1 >= 0 order by f1 limit 1) ss on id = f1;

drop index patest2i;

explain (costs off)
select * from patest0 join (select f1 from int4_tbl where f1 >= 0 order by f1 limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl where f1 >= 0 order by f1 limit 1) ss on id = f1;

drop table patest0 cascade;

--
-- Test merge-append plans for inheritance trees
--

create table matest0 (id serial primary key, name text);
create table matest1 (id integer primary key) inherits (matest0);
create table matest2 (id integer primary key) inherits (matest0);
create table matest3 (id integer primary key) inherits (matest0);

create index matest0i on matest0 ((1-id));
create index matest1i on matest1 ((1-id));
-- create index matest2i on matest2 ((1-id));  -- intentionally missing
create index matest3i on matest3 ((1-id));

insert into matest1 (name) values ('Test 1');
insert into matest1 (name) values ('Test 2');
insert into matest2 (name) values ('Test 3');
insert into matest2 (name) values ('Test 4');
insert into matest3 (name) values ('Test 5');
insert into matest3 (name) values ('Test 6');

set enable_indexscan = off;  -- force use of seqscan/sort, so no merge
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
reset enable_indexscan;

set enable_seqscan = off;  -- plan with fewest seqscans should be merge
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
reset enable_seqscan;

drop table matest0 cascade;

--
-- Test merge-append for UNION ALL append relations
--

set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;

-- Check handling of duplicated, constant, or volatile targetlist items
explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, thousand FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous, thousand+tenthous AS x FROM tenk1
UNION ALL
SELECT 42, 42, hundred FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, random()::integer FROM tenk1
ORDER BY thousand, tenthous;

-- Check min/max aggregate optimization
explain (costs off)
SELECT min(x) FROM
  (SELECT unique1 AS x FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x FROM tenk1 b) s;

explain (costs off)
SELECT min(y) FROM
  (SELECT unique1 AS x, unique1 AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s;

-- XXX planner doesn't recognize that index on unique2 is sufficiently sorted
explain (costs off)
SELECT x, y FROM
  (SELECT thousand AS x, tenthous AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s
ORDER BY x, y;

CREATE TABLE parent1 (aa TEXT) distribute by roundrobin;
CREATE TABLE if not exists child1 (bb TEXT) INHERITS (a) distribute by roundrobin;
drop table parent1;
drop table child1;
reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;
