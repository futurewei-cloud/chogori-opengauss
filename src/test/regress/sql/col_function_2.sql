create schema vector_distribute_function_2;
set current_schema = vector_distribute_function_2;
create table row_functiontable(f1 int, f2 float, f3 text);
insert into row_functiontable values(1,2.0,'abcde'),(2,4.0,'abcde'),(3,5.0,'affde');
insert into row_functiontable values(4,7.0,'aeede'),(5,1.0,'facde'),(6,3.0,'affde');

create table function_table_01(f1 int, f2 float, f3 text)  with (orientation = column);
insert into function_table_01 select * from row_functiontable;

analyze function_table_01;

CREATE OR REPLACE FUNCTION test_function_immutable RETURNS BIGINT AS
$body$ 
BEGIN
RETURN 3;
END;
$body$
LANGUAGE 'plpgsql'
IMMUTABLE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

explain (verbose, costs off) select * from test_function_immutable();
select * from test_function_immutable();
CREATE VIEW functionview AS SELECT f1,f2,left(f3,test_function_immutable()::INT) f3 FROM function_table_01;

CREATE OR REPLACE FUNCTION test_function_volatile RETURNS BIGINT AS
$body$ 
DECLARE
cnt BIGINT;
cnt2 BIGINT;
BEGIN
SELECT count(*) INTO cnt FROM(select * from functionview limit 3);
SELECT count(*) INTO cnt2 from function_table_01;
RETURN (cnt+cnt2/2)/2;
END;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

explain (verbose on, costs off) select * from test_function_volatile();
select * from test_function_volatile();

explain (verbose on, costs off) select test_function_volatile();
select test_function_volatile();

--targetlist
select f1,left(f3,test_function_volatile()::INT) from function_table_01 order by 1 limit 3;

--fromQual
select * from function_table_01 where f1 = test_function_volatile();

--sortClause
select f1,f3 from function_table_01 order by left(f3,test_function_volatile()::INT), f1 limit 3;

--groupClause
select avg(f2),left(f3,test_function_volatile()::INT) from function_table_01 group by 2 order by 1;

--havingClause
select avg(f2) fa,f3 from function_table_01 group by f3 having avg(f2)>test_function_volatile() order by 1;

--limitClause && offsetClause
select * from function_table_01 order by 1  limit test_function_volatile() offset test_function_volatile();

select avg(f2),left(f3,test_function_volatile()::INT) from function_table_01 group by 2 having avg(f2)>test_function_volatile() order by 1 limit test_function_volatile() offset test_function_volatile()-2;

drop function test_function_volatile;
drop view functionview;
drop function test_function_immutable;
drop table function_table_01;
drop table row_functiontable;
drop schema vector_distribute_function_2;
