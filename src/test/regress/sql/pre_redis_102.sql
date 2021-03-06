--partition table
set datestyle = 'iso, ymd';set intervalstyle to postgres;set time zone prc;

create schema redistable;
create table redistable.redis_table_000 (c_int integer, c_smallint smallint, c_bigint bigint, c_decimal decimal , c_numeric numeric , c_real real , c_double double precision , c_money money , c_character_varying character varying(10) , c_varchar varchar(10) , c_char char(10) , c_text text , c_bytea bytea , c_timestamp_without  timestamp without time zone , c_timestamp_with timestamp with time zone , c_boolean boolean ,  c_cidr cidr , c_inet inet ,c_macaddr macaddr , c_bit bit(20) , c_bit_varying bit varying(20) , c_oid oid , c_regproc regproc , c_regprocedure regprocedure , c_regoperator regoperator , c_regclass regclass , c_regtype regtype , c_character character(10) , c_interval interval , c_date date , c_time_without time without time zone, c_time_with time with time zone,c_binary_integer binary_integer,c_binary_double binary_double,c_dec dec(18,9),c_numeric_1 numeric(19,9),c_raw raw,c_varchar2 varchar2) distribute by replication;

create index redis_index_000 on redistable.redis_table_000(c_int,c_bigint);
create view redistable.redis_view_000 as select * from redistable.redis_table_000 where c_smallint > 1;

create table redistable.redis_table_0000 (c_int8 int8,c_int2 int2,c_oid oid,c_int4 int4,c_bool bool,c_int2vector int2vector,c_oidvector oidvector,c_char char(10),c_name name,c_text text,c_bpchar bpchar,c_bytea bytea,c_varchar varchar(20),c_float4 float4,c_float8 float8,c_numeric numeric,c_abstime abstime,c_reltime reltime,c_date date,c_time time,c_timestamp timestamp,c_timestamptz timestamptz,c_interval interval,c_timetz timetz,c_box box,c_money money,c_tsvector tsvector);

--I1.create table and insert into data
create table redistable.redis_table_010_partition_01 (c_int integer, c_smallint smallint, c_bigint bigint, c_decimal decimal , c_numeric numeric , c_real real , c_double double precision , c_money money , c_character_varying character varying(10) , c_varchar varchar(10) , c_char char(10) , c_text text , c_bytea bytea , c_timestamp_without  timestamp without time zone , c_timestamp_with timestamp with time zone , c_boolean boolean ,  c_cidr cidr , c_inet inet ,c_macaddr macaddr , c_bit bit(20) , c_bit_varying bit varying(20) , c_oid oid , c_regproc regproc , c_regprocedure regprocedure , c_regoperator regoperator , c_regclass regclass , c_regtype regtype , c_character character(10) , c_interval interval , c_date date , c_time_without time without time zone, c_time_with time with time zone,c_binary_integer binary_integer,c_binary_double binary_double,c_dec dec(18,9),c_numeric_1 numeric(19,9),c_raw raw,c_varchar2 varchar2) distribute by replication
partition by range(c_smallint)
(
 partition redis_table_010_partition_p1 values less than (6),
 partition redis_table_010_partition_p2 values less than (8),
 partition redis_table_010_partition_p3 values less than (134),
 partition redis_table_010_partition_p4 values less than (maxvalue)
);

insert into redistable.redis_table_010_partition_01 select * from redistable.redis_table_000;
select count(*) from redistable.redis_table_010_partition_01;  --212

create table redistable.redis_table_010_partition_02 (c_int integer, c_smallint smallint, c_bigint bigint, c_decimal decimal , c_numeric numeric , c_real real , c_double double precision , c_money money , c_character_varying character varying(10) , c_varchar varchar(10) , c_char char(10) , c_text text , c_bytea bytea , c_timestamp_without  timestamp without time zone , c_timestamp_with timestamp with time zone , c_boolean boolean ,  c_cidr cidr , c_inet inet ,c_macaddr macaddr , c_bit bit(20) , c_bit_varying bit varying(20) , c_oid oid , c_regproc regproc , c_regprocedure regprocedure , c_regoperator regoperator , c_regclass regclass , c_regtype regtype , c_character character(10) , c_interval interval , c_date date , c_time_without time without time zone, c_time_with time with time zone,c_binary_integer binary_integer,c_binary_double binary_double,c_dec dec(18,9),c_numeric_1 numeric(19,9),c_raw raw,c_varchar2 varchar2)
partition by range(c_bigint)
(
 partition redis_table_010_partition_p1 values less than (6),
 partition redis_table_010_partition_p2 values less than (8),
 partition redis_table_010_partition_p3 values less than (12),
 partition redis_table_010_partition_p4 values less than (15),
 partition redis_table_010_partition_p5 values less than (18),
 partition redis_table_010_partition_p6 values less than (21),
 partition redis_table_010_partition_p7 values less than (36),
 partition redis_table_010_partition_p8 values less than (48),
 partition redis_table_010_partition_p9 values less than (59),
 partition redis_table_010_partition_p10 values less than (72),
 partition redis_table_010_partition_p11 values less than (maxvalue)
);

create unique index redis_index_010_partition_1 on redistable.redis_table_010_partition_02(c_int,c_bigint) local;
create unique index redis_index_010_partition_2 on redistable.redis_table_010_partition_02(c_int,c_smallint,c_bigint,c_decimal) local;
insert into redistable.redis_table_010_partition_02 select * from redistable.redis_table_000 where c_int > 0;
update redistable.redis_table_010_partition_02 set c_varchar2='svn' where c_int=2;
update redistable.redis_table_010_partition_02 set c_varchar2='git' where c_int=1;
reindex index redistable.redis_index_010_partition_2;

--I2.select to verify
select count(*),null as redis_table_010_partition_01,sum(c_int) as c_int_sum,avg(c_int) as c_int_avg,sum(c_smallint) as c_smallint_sum,avg(c_smallint) as c_smallint_avg,sum(c_bigint) as c_bigint_sum,avg(c_bigint) as c_bigint_avg,sum(c_decimal) as c_decimal_sum,avg(c_decimal) as c_decimal_avg,sum(c_numeric) as c_numeric_sum,avg(c_numeric) as c_numeric_avg,max(length(c_character_varying)) as c_character_varying_length_max,min(length(c_char)) as c_char_length_min,sum(length(c_bit_varying)) as c_bit_varying_length_sum from redistable.redis_table_010_partition_01 where c_int <= 201 and c_smallint >= 45 or c_bigint < 10201 and c_smallint != 1 and c_smallint != 16;
select distinct(c_int),null as redis_table_010_partition_01,max(distinct(c_bigint)),c_decimal from redistable.redis_table_010_partition_01 where c_int in (select distinct(c_int) from redistable.redis_table_010_partition_01 where c_smallint != -6) and c_oid >=15 group by c_int,c_decimal order by c_int limit 11;
select count(*),null as redis_table_010_partition_02,sum(c_int) as c_int_sum,avg(c_int) as c_int_avg,sum(c_smallint) as c_smallint_sum,avg(c_smallint) as c_smallint_avg,sum(c_bigint) as c_bigint_sum,avg(c_bigint) as c_bigint_avg,sum(c_decimal) as c_decimal_sum,avg(c_decimal) as c_decimal_avg,sum(c_numeric) as c_numeric_sum,avg(c_numeric) as c_numeric_avg,max(length(c_character_varying)) as c_character_varying_length_max,min(length(c_char)) as c_char_length_min,sum(length(c_bit_varying)) as c_bit_varying_length_sum from redistable.redis_table_010_partition_02 where c_int <= 201 and c_smallint >= 45 or c_bigint < 10201 and c_smallint != 1 and c_smallint != 16;
select distinct(c_int),null as redis_table_010_partition_02,max(distinct(c_bigint)),c_decimal from redistable.redis_table_010_partition_02 where c_int in (select distinct(c_int) from redistable.redis_table_010_partition_02 where c_smallint != -6) and c_oid >=15 group by c_int,c_decimal order by c_int limit 11;

--join 
select count(*),sum(table_compress2.c_int) as c_int_sum,avg(table_compress2.c_int) as c_int_avg,sum(table_compress2.c_smallint) as c_smallint_sum,avg(table_compress1.c_smallint) as c_smallint_avg,sum(table_compress1.c_bigint) as c_bigint_sum,avg(table_compress2.c_bigint) as c_bigint_avg,sum(table_compress2.c_decimal) as c_decimal_sum,avg(table_compress2.c_decimal) as c_decimal_avg,sum(table_compress1.c_numeric) as c_numeric_sum,avg(table_compress1.c_numeric) as c_numeric_avg,max(length(table_compress2.c_character_varying)) as c_character_varying_length_max,min(length(table_compress1.c_char)) as c_char_length_min,sum(length(table_compress2.c_bit_varying)) as c_bit_varying_length_sum from redistable.redis_table_010_partition_02 as table_compress2 inner join redistable.redis_table_010_partition_01 as table_compress1 on table_compress2.c_int < 200 and table_compress1.c_int>10 and table_compress2.c_smallint != table_compress1.c_smallint;
select count(*),sum(table_000.c_int) as c_int_sum,avg(table_000.c_int) as c_int_avg,sum(table_000.c_smallint) as c_smallint_sum,avg(table_compress1.c_smallint) as c_smallint_avg,sum(table_compress1.c_bigint) as c_bigint_sum,avg(table_000.c_bigint) as c_bigint_avg,sum(table_000.c_decimal) as c_decimal_sum,avg(table_000.c_decimal) as c_decimal_avg,sum(table_compress1.c_numeric) as c_numeric_sum,avg(table_compress1.c_numeric) as c_numeric_avg,max(length(table_000.c_character_varying)) as c_character_varying_length_max,min(length(table_compress1.c_char)) as c_char_length_min,sum(length(table_000.c_bit_varying)) as c_bit_varying_length_sum from redistable.redis_table_000 as table_000 inner join redistable.redis_table_010_partition_01 as table_compress1 on table_000.c_int < 200 and table_compress1.c_int>10 and table_000.c_smallint != table_compress1.c_smallint;
select count(*),sum(table_compress2.c_int) as c_int_sum,avg(table_compress2.c_int) as c_int_avg,sum(table_compress2.c_smallint) as c_smallint_sum,avg(table_000.c_smallint) as c_smallint_avg,sum(table_000.c_bigint) as c_bigint_sum,avg(table_compress2.c_bigint) as c_bigint_avg,sum(table_compress2.c_decimal) as c_decimal_sum,avg(table_compress2.c_decimal) as c_decimal_avg,sum(table_000.c_numeric) as c_numeric_sum,avg(table_000.c_numeric) as c_numeric_avg,max(length(table_compress2.c_character_varying)) as c_character_varying_length_max,min(length(table_000.c_char)) as c_char_length_min,sum(length(table_compress2.c_bit_varying)) as c_bit_varying_length_sum from redistable.redis_table_010_partition_02 as table_compress2 inner join redistable.redis_table_000 as table_000 on table_compress2.c_int < 200 and table_000.c_int>10 and table_compress2.c_smallint != table_000.c_smallint;

DROP SCHEMA redistable CASCADE;
