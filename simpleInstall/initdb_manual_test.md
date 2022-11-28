
# initdb manual test
This manual test guide shows how to manually follow the steps that initdb will continue with after bootstrap_template1().

"--" is used to start non-SQL descriptions, as they will be treated as comments in the backend terminal. Multiline statements are spread to multiple lines with the "\\" symbol. It is organized in this way, so that one can potentially copy and paste the markdown content to the backend terminal for execution directly.


-- __1. Setup auth__
REVOKE ALL on pg_authid FROM public;
ALTER USER omm WITH PASSWORD E'Test3456'; -- This generates a warning, but seems OK.
<br>

-- __2. Setup depend, load plpgsql, and system views__
-- 2.1. Setup depend
DELETE FROM pg_depend;
-- VACUUM pg_depend; -- this should be commented out
DELETE FROM pg_shdepend;
-- VACUUM pg_shdepend; -- this should be commented out
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_class;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_type;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_cast;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_constraint;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_attrdef;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_language;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_operator;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_opclass;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_opfamily;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_amop;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_amproc;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_rewrite;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_trigger;
-- -- Restriction here to avoid pinning the public namespace
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_namespace WHERE nspname LIKE 'pg%' or nspname = 'cstore';
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_parser;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_dict;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_template;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_config;
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_collation;
INSERT INTO pg_shdepend SELECT 0,0,0,0, tableoid,oid, 'p' FROM pg_authid;
SELECT '======== Setup depend finished ========';

-- 2.2 load plpgsql
CREATE EXTENSION plpgsql;
SELECT '======== load plpgsql finished ========';

-- 2.3 Setup system views
-- -- Run all statements within src/common/backend/catalog/system_views.sql
<br>

-- __3. Setup perf views__
-- -- Run all statements within src/common/backend/catalog/performance_views.sql
<br>

-- __4. Setup description__
-- Note: need to set the value for buf_file, which is "postgres.description".
CREATE TEMP TABLE tmp_pg_description (\\
&nbsp;&nbsp;&nbsp;&nbsp; objoid oid,\\
&nbsp;&nbsp;&nbsp;&nbsp;     classname name,\\
&nbsp;&nbsp;&nbsp;&nbsp;     objsubid int4,\\
&nbsp;&nbsp;&nbsp;&nbsp;     description text) WITHOUT OIDS;

COPY tmp_pg_description FROM E'postgres.description';

INSERT INTO pg_description \\
&nbsp;&nbsp;&nbsp;&nbsp; SELECT t.objoid, c.oid, t.objsubid, t.description \\
&nbsp;&nbsp;&nbsp;&nbsp; FROM tmp_pg_description t, pg_class c WHERE c.relname = t.classname;

CREATE TEMP TABLE tmp_pg_shdescription (\\
&nbsp;&nbsp;&nbsp;&nbsp; objoid oid,\\
&nbsp;&nbsp;&nbsp;&nbsp; classname name,\\
&nbsp;&nbsp;&nbsp;&nbsp; description text) WITHOUT OIDS;

COPY tmp_pg_shdescription FROM E'postgres.shdescription'; 

INSERT INTO pg_shdescription\\
&nbsp;&nbsp;&nbsp;&nbsp; SELECT t.objoid, c.oid, t.description\\
&nbsp;&nbsp;&nbsp;&nbsp; FROM tmp_pg_shdescription t, pg_class c\\
&nbsp;&nbsp;&nbsp;&nbsp; WHERE c.relname = t.classname;

-- Create default descriptions for operator implementation functions
WITH funcdescs AS (\\
SELECT p.oid as p_oid, oprname,\\
coalesce(obj_description(o.oid, 'pg_operator'),'') as opdesc\\
FROM pg_proc p JOIN pg_operator o ON oprcode = p.oid )\\
INSERT INTO pg_description\\
&nbsp;&nbsp;&nbsp;&nbsp; SELECT p_oid, 'pg_proc'::regclass, 0, 'implementation of ' || oprname || ' operator'\\
&nbsp;&nbsp;&nbsp;&nbsp;FROM funcdescs\\
&nbsp;&nbsp;&nbsp;&nbsp;WHERE opdesc NOT LIKE 'deprecated%' AND\\
&nbsp;&nbsp;&nbsp;&nbsp;NOT EXISTS (SELECT 1 FROM pg_description\\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHERE objoid = p_oid AND classoid = 'pg_proc'::regclass);
<br>

-- __5. Setup collation__
-- Note: not sure if this step is needed.
-- Note: need to find out values for "quoted_locale", "enc".
CREATE TEMP TABLE tmp_pg_collation (\\
&nbsp;&nbsp;&nbsp;&nbsp;collname name,\\
&nbsp;&nbsp;&nbsp;&nbsp;locale name,\\
&nbsp;&nbsp;&nbsp;&nbsp;encoding int) WITHOUT OIDS;

-- The next two lines are within a while loop:
INSERT INTO tmp_pg_collation VALUES (E'quoted_locale', E'quoted_locale', enc);
-- INSERT INTO tmp_pg_collation VALUES (E'buf_alias', E'quoted_locale', enc);

INSERT INTO tmp_pg_collation VALUES ('ucs_basic', 'C', 7); -- PG_UTF8 = 7

INSERT INTO pg_collation (collname, collnamespace, collowner, collencoding, collcollate, collctype)\\
&nbsp;&nbsp;&nbsp;&nbsp; SELECT DISTINCT ON (collname, encoding)\\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;collname, \\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') AS collnamespace,\\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(SELECT relowner FROM pg_class WHERE relname = 'pg_collation') AS collowner, \\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;encoding, locale, locale\\
&nbsp;&nbsp;&nbsp;&nbsp;FROM tmp_pg_collation\\
&nbsp;&nbsp;&nbsp;&nbsp;WHERE NOT EXISTS (SELECT 1 FROM pg_collation WHERE collname = tmp_pg_collation.collname)\\
&nbsp;&nbsp;&nbsp;&nbsp;ORDER BY collname, encoding, (collname = locale) DESC, locale;
<br>

-- __6. Setup privileges__
-- Note: not sure whether this is correct to just use omm as the superuser.
UPDATE pg_class\\
&nbsp;&nbsp;&nbsp;&nbsp;SET relacl = E'omm'\\
&nbsp;&nbsp;&nbsp;&nbsp;WHERE relkind IN ('r', 'v', 'm', 'S') AND relacl IS NULL;

-- privileges setup line 1 ~ 6
GRANT USAGE ON SCHEMA pg_catalog TO PUBLIC;
GRANT CREATE, USAGE ON SCHEMA public TO PUBLIC;
REVOKE ALL ON pg_largeobject FROM PUBLIC;
REVOKE ALL on pg_user_status FROM public;
REVOKE ALL on pg_auth_history FROM public;
REVOKE ALL on pg_extension_data_source FROM public;
-- privileges setup line 7
REVOKE ALL on schema public FROM public; -- when security mode is on
-- REVOKE CREATE on schema public FROM public; -- when security mode is off
-- privileges setup line 8 ~ 17
REVOKE ALL on gs_auditing_policy FROM public;
REVOKE ALL on gs_auditing_policy_access FROM public;
REVOKE ALL on gs_auditing_policy_filters FROM public;
REVOKE ALL on gs_auditing_policy_privileges FROM public;
REVOKE ALL on gs_policy_label FROM public;
REVOKE ALL on gs_masking_policy FROM public;
REVOKE ALL on gs_masking_policy_actions FROM public;
REVOKE ALL on gs_masking_policy_filters FROM public;
GRANT USAGE ON SCHEMA sqladvisor TO PUBLIC;
GRANT USAGE ON SCHEMA dbe_pldebugger TO PUBLIC;
<br>

-- __7. Setup bucketmap len__
-- Note: need to set the value for g_bucket_len, which is 16384 by default.
INSERT INTO gs_global_config VALUES ('buckets_len', 16384);
<br>

-- __8. Setup schema__
-- Run all statements from src/common/backend/catalog/information_schema.sql
-- Note: need to set the value for infoversion, which consists of major, minor, micro and letterversion.
-- The strange infoversion seems to be "09.08.0007abc".

UPDATE information_schema.sql_implementation_info \\
&nbsp;&nbsp;&nbsp;&nbsp; SET character_value = '09.08.0007abc' \\
&nbsp;&nbsp;&nbsp;&nbsp; WHERE implementation_info_name = 'DBMS VERSION';

-- Note: need to set the value for buf_features, which is sql_features.txt
COPY information_schema.sql_features\\
&nbsp;&nbsp;&nbsp;&nbsp; (feature_id, feature_name, sub_feature_id,\\
&nbsp;&nbsp;&nbsp;&nbsp; sub_feature_name, is_supported, comments)\\
&nbsp;&nbsp;&nbsp;&nbsp; FROM E'sql_features.txt';
<br>

-- __9. Load update__
-- Add new context into update_systbl.sql to update system tables.
-- TODO: Note: need to set the value for share_path, which seems to be src/common/backend/catalog.
COPY pg_cast FROM 'share_path/pg_cast_oid.txt' USING DELIMITERS '|';
<br>

-- __10. Setup snapshots__
-- Need to read an array of snapshot files, and do the setup in each snapshot file.
-- All snapshot files are stored in the snapshot_files array.
-- Names of all the snapshots are stored in the snapshot_names array: 
-- {"schema", "create", "prepare", "sample", "publish", "purge"}
<br>

-- __Next steps__
-- make_template0()
-- make_postgres()

