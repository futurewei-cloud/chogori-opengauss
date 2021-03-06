create or replace FUNCTION cbm_get_merged_file()
RETURNS table (id text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;
        param2 text;
BEGIN
        SELECT pg_cbm_tracked_location() into param1;

        CREATE TABLE cbmtst (i INT);
        INSERT INTO cbmtst SELECT * FROM generate_series(1,800);
        CREATE TABLE cbmtst1 (j INT);
        INSERT INTO cbmtst1 SELECT * FROM cbmtst;
        CHECKPOINT;
        SELECT pg_cbm_tracked_location() into param2;
        return query SELECT pg_cbm_get_merged_file(''|| param1 || '',''|| param2 ||'');
END;
$$
;

create or replace FUNCTION cbm_get_changed_block()
RETURNS table (merged_start_lsn text,merged_end_lsn text,tablespace_oid oid,database_oid oid, relfilenode oid, fork_number int,path text,rel_dropped bool,rel_created bool, rel_truncated bool,truncate_blocknum oid,changed_block_number oid, changed_block_list text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;
        param2 text;
BEGIN
        SELECT pg_cbm_tracked_location() into param1;
        TRUNCATE cbmtst1;
        CREATE TABLE cbmtst2 (x INT);
        INSERT INTO cbmtst2 SELECT * FROM generate_series(1,800);
        CREATE TABLE cbmtst3 (y INT);
        INSERT INTO cbmtst3 SELECT * FROM cbmtst2;
        CHECKPOINT;
        SELECT pg_cbm_tracked_location() into param2;

        return query SELECT * FROM pg_cbm_get_changed_block(''|| param1 || '',''|| param2 ||'') limit 1;
END;
$$
;

create or replace FUNCTION cbm_force_track()
RETURNS table (id text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;

BEGIN
        DROP TABLE cbmtst2;
        DROP TABLE cbmtst3;

        SELECT pg_current_xlog_location() into param1;
        return query SELECT pg_cbm_force_track(''|| param1 || '',100000);
END;
$$
;

create or replace FUNCTION cbm_recycle_file()
RETURNS table (id text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;

BEGIN
        SELECT pg_cbm_tracked_location() into param1;
        return query SELECT pg_cbm_recycle_file(''|| param1 || '') ;
END;
$$
;

create or replace FUNCTION disable_delay_ddl_recycle()
RETURNS table (i text,j text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;

BEGIN
        SELECT pg_current_xlog_location() into param1;
        DROP TABLE cbmtst;
        DROP TABLE cbmtst1;
        CHECKPOINT;
        return query SELECT * from  pg_disable_delay_ddl_recycle(''|| param1 || '',false);
END;
$$
;

DROP TABLE IF EXISTS cbmtst;
DROP TABLE IF EXISTS cbmtst1;
DROP TABLE IF EXISTS cbmtst2;
DROP TABLE IF EXISTS cbmtst3;
DROP TABLE IF EXISTS timestst1;
SELECT pg_cbm_tracked_location();
select cbm_get_merged_file();
select cbm_get_changed_block();
select cbm_force_track();
select cbm_recycle_file();
SELECT pg_enable_delay_ddl_recycle();
SELECT pg_enable_delay_xlog_recycle();
select disable_delay_ddl_recycle();
SELECT pg_disable_delay_xlog_recycle();

--clean
DROP TABLE IF EXISTS cbmtst;
DROP TABLE IF EXISTS cbmtst1;
DROP TABLE IF EXISTS cbmtst2;
DROP TABLE IF EXISTS cbmtst3;
DROP TABLE IF EXISTS timestst1;
DROP FUNCTION IF EXISTS cbm_get_merged_file;
DROP FUNCTION IF EXISTS cbm_get_changed_block;
DROP FUNCTION IF EXISTS cbm_force_track;
DROP FUNCTION IF EXISTS cbm_recycle_file;
DROP FUNCTION IF EXISTS disable_delay_ddl_recycle;
