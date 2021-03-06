--
-- Sanity checks for text search catalogs
--
-- NB: we assume the oidjoins test will have caught any dangling links,
-- that is OID or REGPROC fields that are not zero and do not match some
-- row in the linked-to table.  However, if we want to enforce that a link
-- field can't be 0, we have to check it here.

-- Find unexpected zero link entries

SELECT oid, prsname
FROM pg_ts_parser
WHERE prsnamespace = 0 OR prsstart = 0 OR prstoken = 0 OR prsend = 0 OR
      -- prsheadline is optional
      prslextype = 0;

SELECT oid, dictname
FROM pg_ts_dict
WHERE dictnamespace = 0 OR dictowner = 0 OR dicttemplate = 0;

SELECT oid, tmplname
FROM pg_ts_template
WHERE tmplnamespace = 0 OR tmpllexize = 0;  -- tmplinit is optional

SELECT oid, cfgname
FROM pg_ts_config
WHERE cfgnamespace = 0 OR cfgowner = 0 OR cfgparser = 0;

SELECT mapcfg, maptokentype, mapseqno
FROM pg_ts_config_map
WHERE mapcfg = 0 OR mapdict = 0;

-- Look for pg_ts_config_map entries that aren't one of parser's token types
SELECT * FROM
  ( SELECT oid AS cfgid, (ts_token_type(cfgparser)).tokid AS tokid
    FROM pg_ts_config ) AS tt
RIGHT JOIN pg_ts_config_map AS m
    ON (tt.cfgid=m.mapcfg AND tt.tokid=m.maptokentype)
WHERE
    tt.cfgid IS NULL OR tt.tokid IS NULL;

-- test basic text search behavior without indexes, then with

SELECT count(*) FROM test_tsvector WHERE a @@ 'wr|qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'wr&qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq&yt';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq|yt';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq|yt)&(wr|qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ 'w:*|q:*';

create index wowidx on test_tsvector using gist (a);

SET enable_seqscan=OFF;

SELECT count(*) FROM test_tsvector WHERE a @@ 'wr|qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'wr&qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq&yt';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq|yt';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq|yt)&(wr|qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ 'w:*|q:*';
SELECT count(*) FROM test_tsvector WHERE a @@ any ('{wr,qh}');

RESET enable_seqscan;

DROP INDEX wowidx;

CREATE INDEX wowidx ON test_tsvector USING gin (a);

SET enable_seqscan=OFF;

SELECT count(*) FROM test_tsvector WHERE a @@ 'wr|qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'wr&qh';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq&yt';
SELECT count(*) FROM test_tsvector WHERE a @@ 'eq|yt';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ '(eq|yt)&(wr|qh)';
SELECT count(*) FROM test_tsvector WHERE a @@ 'w:*|q:*';
SELECT count(*) FROM test_tsvector WHERE a @@ any ('{wr,qh}');

RESET enable_seqscan;
INSERT INTO test_tsvector VALUES ('???', 'DFG:1A,2B,6C,10 FGH');
SELECT * FROM ts_stat('SELECT a FROM test_tsvector') ORDER BY ndoc DESC, nentry DESC, word LIMIT 10;
SELECT * FROM ts_stat('SELECT a FROM test_tsvector', 'AB') ORDER BY ndoc DESC, nentry DESC, word;

--dictionaries and to_tsvector

SELECT ts_lexize('english_stem', 'skies');
SELECT ts_lexize('english_stem', 'identity');

SELECT * FROM ts_token_type('default') ORDER BY tokid;

SELECT * FROM ts_parse('default', '345 qwe@efd.r '' http://www.com/ http://aew.werc.ewr/?ad=qwe&dw 1aew.werc.ewr/?ad=qwe&dw 2aew.werc.ewr http://3aew.werc.ewr/?ad=qwe&dw http://4aew.werc.ewr http://5aew.werc.ewr:8100/?  ad=qwe&dw 6aew.werc.ewr:8100/?ad=qwe&dw 7aew.werc.ewr:8100/?ad=qwe&dw=%20%32 +4.0e-10 qwe qwe qwqwe 234.435 455 5.005 teodor@stack.net qwe-wer asdf <fr>qwer jf sdjk<we hjwer <werrwe> ewr1> ewri2 <a href="qwe<qwe>">
/usr/local/fff /awdf/dwqe/4325 rewt/ewr wefjn /wqe-324/ewr gist.h gist.h.c gist.c. readline 4.2 4.2. 4.2, readline-4.2 readline-4.2. 234
<i <b> wow  < jqw <> qwerty') ORDER BY tokid,token collate "POSIX";

SELECT to_tsvector('english', '345 qwe@efd.r '' http://www.com/ http://aew.werc.ewr/?ad=qwe&dw 1aew.werc.ewr/?ad=qwe&dw 2aew.werc.ewr http://3aew.werc.ewr/?ad=qwe&dw http://4aew.werc.ewr http://5aew.werc.ewr:8100/?  ad=qwe&dw 6aew.werc.ewr:8100/?ad=qwe&dw 7aew.werc.ewr:8100/?ad=qwe&dw=%20%32 +4.0e-10 qwe qwe qwqwe 234.435 455 5.005 teodor@stack.net qwe-wer asdf <fr>qwer jf sdjk<we hjwer <werrwe> ewr1> ewri2 <a href="qwe<qwe>">
/usr/local/fff /awdf/dwqe/4325 rewt/ewr wefjn /wqe-324/ewr gist.h gist.h.c gist.c. readline 4.2 4.2. 4.2, readline-4.2 readline-4.2. 234
<i <b> wow  < jqw <> qwerty');

SELECT length(to_tsvector('english', '345 qwe@efd.r '' http://www.com/ http://aew.werc.ewr/?ad=qwe&dw 1aew.werc.ewr/?ad=qwe&dw 2aew.werc.ewr http://3aew.werc.ewr/?ad=qwe&dw http://4aew.werc.ewr http://5aew.werc.ewr:8100/?  ad=qwe&dw 6aew.werc.ewr:8100/?ad=qwe&dw 7aew.werc.ewr:8100/?ad=qwe&dw=%20%32 +4.0e-10 qwe qwe qwqwe 234.435 455 5.005 teodor@stack.net qwe-wer asdf <fr>qwer jf sdjk<we hjwer <werrwe> ewr1> ewri2 <a href="qwe<qwe>">
/usr/local/fff /awdf/dwqe/4325 rewt/ewr wefjn /wqe-324/ewr gist.h gist.h.c gist.c. readline 4.2 4.2. 4.2, readline-4.2 readline-4.2. 234
<i <b> wow  < jqw <> qwerty'));

-- ts_debug

SELECT * from ts_debug('english', '<myns:foo-bar_baz.blurfl>abc&nm1;def&#xa9;ghi&#245;jkl</myns:foo-bar_baz.blurfl>') ORDER BY alias, description, token collate "POSIX";

-- check parsing of URLs
SELECT * from ts_debug('english', 'http://www.harewoodsolutions.co.uk/press.aspx</span>');
SELECT * from ts_debug('english', 'http://aew.wer0c.ewr/id?ad=qwe&dw<span>');
SELECT * from ts_debug('english', 'http://5aew.werc.ewr:8100/?');
SELECT * from ts_debug('english', '5aew.werc.ewr:8100/?xx');

-- to_tsquery

SELECT to_tsquery('english', 'qwe & sKies ');
SELECT to_tsquery('simple', 'qwe & sKies ');
SELECT to_tsquery('english', '''the wether'':dc & ''           sKies '':BC ');
SELECT to_tsquery('english', 'asd&(and|fghj)');
SELECT to_tsquery('english', '(asd&and)|fghj');
SELECT to_tsquery('english', '(asd&!and)|fghj');
SELECT to_tsquery('english', '(the|and&(i&1))&fghj');

SELECT plainto_tsquery('english', 'the and z 1))& fghj');
SELECT plainto_tsquery('english', 'foo bar') && plainto_tsquery('english', 'asd');
SELECT plainto_tsquery('english', 'foo bar') || plainto_tsquery('english', 'asd fg');
SELECT plainto_tsquery('english', 'foo bar') || !!plainto_tsquery('english', 'asd fg');
SELECT plainto_tsquery('english', 'foo bar') && 'asd | fg';

SELECT ts_rank_cd(to_tsvector('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
'), to_tsquery('english', 'paint&water'));

SELECT ts_rank_cd(to_tsvector('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
'), to_tsquery('english', 'breath&motion&water'));

SELECT ts_rank_cd(to_tsvector('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
'), to_tsquery('english', 'ocean'));

--headline tests
SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'paint&water'));

SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'breath&motion&water'));

SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'ocean'));

SELECT ts_headline('english', '
<html>
<!-- some comment -->
<body>
Sea view wow <u>foo bar</u> <i>qq</i>
<a href="http://www.google.com/foo.bar.html" target="_blank">YES &nbsp;</a>
ff-bg
<script>
       document.write(15);
</script>
</body>
</html>',
to_tsquery('english', 'sea&foo'), 'HighlightAll=true');

--Check if headline fragments work
SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'ocean'), 'MaxFragments=1');

--Check if more than one fragments are displayed
SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'Coleridge & stuck'), 'MaxFragments=2');

--Fragments when there all query words are not in the document
SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'ocean & seahorse'), 'MaxFragments=1');

--FragmentDelimiter option
SELECT ts_headline('english', '
Day after day, day after day,
  We stuck, nor breath nor motion,
As idle as a painted Ship
  Upon a painted Ocean.
Water, water, every where
  And all the boards did shrink;
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'Coleridge & stuck'), 'MaxFragments=2,FragmentDelimiter=***');

--Rewrite sub system

CREATE TABLE test_tsquery (txtkeyword TEXT, txtsample TEXT);
\set ECHO none
\copy test_tsquery from stdin
'New York'	new & york | big & apple | nyc
Moscow	moskva | moscow
'Sanct Peter'	Peterburg | peter | 'Sanct Peterburg'
'foo bar qq'	foo & (bar | qq) & city
\.
\set ECHO all

ALTER TABLE test_tsquery ADD COLUMN keyword tsquery;
UPDATE test_tsquery SET keyword = to_tsquery('english', txtkeyword);
ALTER TABLE test_tsquery ADD COLUMN sample tsquery;
UPDATE test_tsquery SET sample = to_tsquery('english', txtsample::text);


SELECT COUNT(*) FROM test_tsquery WHERE keyword <  'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword <= 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword = 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword >= 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword >  'new & york';

CREATE UNIQUE INDEX bt_tsq ON test_tsquery (keyword);

SET enable_seqscan=OFF;

SELECT COUNT(*) FROM test_tsquery WHERE keyword <  'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword <= 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword = 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword >= 'new & york';
SELECT COUNT(*) FROM test_tsquery WHERE keyword >  'new & york';

RESET enable_seqscan;

SELECT ts_rewrite('foo & bar & qq & new & york',  'new & york'::tsquery, 'big & apple | nyc | new & york & city');

SELECT ts_rewrite('moscow', 'SELECT keyword, sample FROM test_tsquery'::text );
SELECT ts_rewrite('moscow & hotel', 'SELECT keyword, sample FROM test_tsquery'::text );
SELECT ts_rewrite('bar & new & qq & foo & york', 'SELECT keyword, sample FROM test_tsquery'::text );

SELECT ts_rewrite( 'moscow', 'SELECT keyword, sample FROM test_tsquery');
SELECT ts_rewrite( 'moscow & hotel', 'SELECT keyword, sample FROM test_tsquery');
SELECT ts_rewrite( 'bar & new & qq & foo & york', 'SELECT keyword, sample FROM test_tsquery');


SELECT keyword FROM test_tsquery WHERE keyword @> 'new';
SELECT keyword FROM test_tsquery WHERE keyword @> 'moscow';
SELECT keyword FROM test_tsquery WHERE keyword <@ 'new';
SELECT keyword FROM test_tsquery WHERE keyword <@ 'moscow';
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow & hotel') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'bar &  new & qq & foo & york') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow & hotel') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'bar & new & qq & foo & york') AS query;

CREATE INDEX qq ON test_tsquery USING gist (keyword tsquery_ops);
SET enable_seqscan=OFF;

SELECT keyword FROM test_tsquery WHERE keyword @> 'new';
SELECT keyword FROM test_tsquery WHERE keyword @> 'moscow';
SELECT keyword FROM test_tsquery WHERE keyword <@ 'new';
SELECT keyword FROM test_tsquery WHERE keyword <@ 'moscow';
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow & hotel') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'bar & new & qq & foo & york') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'moscow & hotel') AS query;
SELECT ts_rewrite( query, 'SELECT keyword, sample FROM test_tsquery' ) FROM to_tsquery('english', 'bar &  new & qq & foo & york') AS query;

RESET enable_seqscan;

--test GUC
SET default_text_search_config=simple;

SELECT to_tsvector('SKIES My booKs');
SELECT plainto_tsquery('SKIES My booKs');
SELECT to_tsquery('SKIES & My | booKs');

SET default_text_search_config=english;

SELECT to_tsvector('SKIES My booKs');
SELECT plainto_tsquery('SKIES My booKs');
SELECT to_tsquery('SKIES & My | booKs');

--trigger
CREATE TRIGGER tsvectorupdate
BEFORE UPDATE OR INSERT ON test_tsvector
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english', t);

SELECT count(*) FROM test_tsvector WHERE a @@ to_tsquery('345&qwerty');
INSERT INTO test_tsvector (t) VALUES ('345 qwerty');
SELECT count(*) FROM test_tsvector WHERE a @@ to_tsquery('345&qwerty');
UPDATE test_tsvector SET t = null WHERE t = '345 qwerty';
SELECT count(*) FROM test_tsvector WHERE a @@ to_tsquery('345&qwerty');

INSERT INTO test_tsvector (t) VALUES ('345 qwerty');

SELECT count(*) FROM test_tsvector WHERE a @@ to_tsquery('345&qwerty');

-- Enforce use of COMMIT instead of 2PC for temporary objects

-- test finding items in GIN's pending list
create temp table pendtest (ts tsvector);
create index pendtest_idx on pendtest using gin(ts);
insert into pendtest values (to_tsvector('Lore ipsam'));
insert into pendtest values (to_tsvector('Lore ipsum'));
select * from pendtest where 'ipsu:*'::tsquery @@ ts;
select * from pendtest where 'ipsa:*'::tsquery @@ ts;
select * from pendtest where 'ips:*'::tsquery @@ ts ORDER BY 1;
select * from pendtest where 'ipt:*'::tsquery @@ ts;
select * from pendtest where 'ipi:*'::tsquery @@ ts;


--added for llt
--test_tsvector tsvector_update_trigger


drop table test_tsvector_llt;
create table test_tsvector_llt(a int, b int);
insert into test_tsvector_llt values (1,2);

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH STATEMENT   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english', t);
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
after UPDATE OR INSERT ON test_tsvector_llt
FOR EACH row   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english', t);
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE delete on test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english', t);
delete from test_tsvector_llt;
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english');
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(b, 'pg_catalog.english',t);
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(c, 'pg_catalog.english',t);
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

drop table test_tsvector_llt;
create table test_tsvector_llt(a tsvector, b int);

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(b, 'pg_catalog.english',t);
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english',t);
insert into test_tsvector_llt values('1',2);
 drop trigger tsvectorupdate_llt on test_tsvector_llt;
 
CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(a, 'pg_catalog.english',b); 
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger(a, 't',b); 
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;


CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger_column(a, 't',b); 
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger_column(a, 'a',b); 
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

drop table test_tsvector_llt;
create table test_tsvector_llt(a tsvector, b regconfig);

CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger_column(a, 'b',b); 
insert into test_tsvector_llt values('1',2);
drop trigger tsvectorupdate_llt on test_tsvector_llt;

drop table test_tsvector_llt;
create table test_tsvector_llt(a tsvector, b regconfig default NULL);
CREATE TRIGGER tsvectorupdate_llt
BEFORE UPDATE OR INSERT ON test_tsvector_llt
FOR EACH ROW   EXECUTE PROCEDURE tsvector_update_trigger_column(a, 'b',b); 
insert into test_tsvector_llt(a) values('1');
drop trigger tsvectorupdate_llt on test_tsvector_llt;

select tsvector_update_trigger();

drop table test_tsvector_llt;

--test long_int_add
select to_number('00000000000','xxxxxxxxxxxxxxxxxx');

--test DecodeTime
select timestamp_in('123456789000998646:56:45',1114,-1);
select timestamp_in('12:123456789000998646:56',1114,-1);

--test examine_simple_variable
create table test_part1(a int,b int) partition by range (a)(partition p1  values less than(100));
create table test_part2(a int,b int) partition by range (b)(partition p2 values less than(100));
select * from test_part1 partition (p1),test_part2 partition (p2) where test_part1.a=test_part2.b;
drop table test_part1;
drop table test_part2;

--test string_to_bytea_const
CREATE TABLE srt ( name bytea);
INSERT INTO srt (name)
VALUES ('aardvark'),
       ('AAA'),
       ('aba'),
       ('ABC'),
       ('abd');
SELECT name FROM srt WHERE name ~~  '%a%' ORDER BY name;
drop table srt;


--test _outRemoteQuery
drop table dept_llt;
CREATE TABLE dept_llt(DNAME VARCHAR(100), LOC VARCHAR(100), DEPTNO INTEGER);
set Debug_print_plan = on;
select * from dept_llt;
reset Debug_print_plan;
drop table dept_llt;