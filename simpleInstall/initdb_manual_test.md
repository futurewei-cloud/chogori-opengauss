
# initdb manual test
This manual test guide shows how to manually follow the steps that initdb will continue with after bootstrap_template1().

"--" is used to start non-SQL descriptions, as they will be treated as comments in the backend terminal. Multiline statements are spread into multiple lines with the "\\" symbol. It is organized in this way, so that one can potentially have the convenience of copying and pasting the markdown content into the backend terminal for execution directly.<br>
<br>

-- __1. Setup auth__<br>
REVOKE ALL on pg_authid FROM public;<br>
ALTER USER omm WITH PASSWORD E'Test3456'; -- This may generate a warning, but seems OK <br>
<br>

-- __2. Setup depend, load plpgsql, and system views__<br>
-- 2.1. Setup depend<br>
DELETE FROM pg_depend;<br>
-- VACUUM pg_depend; -- this should be commented out<br>
DELETE FROM pg_shdepend;<br>
-- VACUUM pg_shdepend; -- this should be commented out<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_class;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_type;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_cast;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_constraint;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_attrdef;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_language;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_operator;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_opclass;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_opfamily;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_amop;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_amproc;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_rewrite;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_trigger;<br>
-- -- Restriction here to avoid pinning the public namespace<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_namespace WHERE nspname LIKE 'pg%' or nspname = 'cstore';<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_parser;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_dict;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_template;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_ts_config;<br>
INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' FROM pg_collation;<br>
INSERT INTO pg_shdepend SELECT 0,0,0,0, tableoid,oid, 'p' FROM pg_authid;<br>
-- 2.2 load plpgsql<br>
CREATE EXTENSION plpgsql;<br>
-- 2.3 Setup system views<br>
-- -- Run all statements within src/common/backend/catalog/system_views.sql<br>
<br>

-- __3. Setup perf views__<br>
-- -- Run all statements within src/common/backend/catalog/performance_views.sql
<br>
<br>

-- __4. Setup description__<br>
-- Note: need to set the value for buf_file, which is "postgres.description".<br>
CREATE TEMP TABLE tmp_pg_description (\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; objoid oid,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;     classname name,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;     objsubid int4,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;     description text) WITHOUT OIDS;<br>

COPY tmp_pg_description FROM E'postgres.description';<br>

INSERT INTO pg_description \\<br>
&nbsp;&nbsp;&nbsp;&nbsp; SELECT t.objoid, c.oid, t.objsubid, t.description \\<br>
&nbsp;&nbsp;&nbsp;&nbsp; FROM tmp_pg_description t, pg_class c WHERE c.relname = t.classname;<br>

CREATE TEMP TABLE tmp_pg_shdescription (\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; objoid oid,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; classname name,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; description text) WITHOUT OIDS;<br>

COPY tmp_pg_shdescription FROM E'postgres.shdescription'; <br>

INSERT INTO pg_shdescription\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; SELECT t.objoid, c.oid, t.description\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; FROM tmp_pg_shdescription t, pg_class c\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; WHERE c.relname = t.classname;<br>

-- Create default descriptions for operator implementation functions<br>
WITH funcdescs AS (\\<br>
SELECT p.oid as p_oid, oprname,\\<br>
coalesce(obj_description(o.oid, 'pg_operator'),'') as opdesc\\<br>
FROM pg_proc p JOIN pg_operator o ON oprcode = p.oid )\\<br>
INSERT INTO pg_description\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; SELECT p_oid, 'pg_proc'::regclass, 0, 'implementation of ' || oprname || ' operator'\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;FROM funcdescs\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;WHERE opdesc NOT LIKE 'deprecated%' AND\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;NOT EXISTS (SELECT 1 FROM pg_description\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHERE objoid = p_oid AND classoid = 'pg_proc'::regclass);<br>
<br>

-- __5. Setup collation__<br>
-- Note: not sure if this step is needed.<br>

-- TODO: Note: need to find out values for "quoted_locale", "enc".<br>
CREATE TEMP TABLE tmp_pg_collation (\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;collname name,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;locale name,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;encoding int) WITHOUT OIDS;<br>

-- The next two lines are within a while loop:<br>
INSERT INTO tmp_pg_collation VALUES (E'quoted_locale', E'quoted_locale', enc);<br>
-- INSERT INTO tmp_pg_collation VALUES (E'buf_alias', E'quoted_locale', enc);<br>
<br>
INSERT INTO tmp_pg_collation VALUES ('ucs_basic', 'C', 7); -- PG_UTF8 = 7<br>

INSERT INTO pg_collation (collname, collnamespace, collowner, collencoding, collcollate, collctype)\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; SELECT DISTINCT ON (collname, encoding)\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;collname, \\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') AS collnamespace,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(SELECT relowner FROM pg_class WHERE relname = 'pg_collation') AS collowner, \\<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;encoding, locale, locale\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;FROM tmp_pg_collation\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;WHERE NOT EXISTS (SELECT 1 FROM pg_collation WHERE collname = tmp_pg_collation.collname)\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;ORDER BY collname, encoding, (collname = locale) DESC, locale;
<br>
<br>

-- __6. Setup privileges__<br>
-- Note: not sure whether this is correct to just use omm as the superuser.<br>
UPDATE pg_class\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;SET relacl = E'omm'\\<br>
&nbsp;&nbsp;&nbsp;&nbsp;WHERE relkind IN ('r', 'v', 'm', 'S') AND relacl IS NULL;<br>

-- privileges setup line 1 ~ 6<br>
GRANT USAGE ON SCHEMA pg_catalog TO PUBLIC;<br>
GRANT CREATE, USAGE ON SCHEMA public TO PUBLIC;<br>
REVOKE ALL ON pg_largeobject FROM PUBLIC;<br>
REVOKE ALL on pg_user_status FROM public;<br>
REVOKE ALL on pg_auth_history FROM public;<br>
REVOKE ALL on pg_extension_data_source FROM public;<br>
-- privileges setup line 7<br>
REVOKE ALL on schema public FROM public; -- when security mode is on<br>
-- REVOKE CREATE on schema public FROM public; -- when security mode is off<br>
-- privileges setup line 8 ~ 17<br>
REVOKE ALL on gs_auditing_policy FROM public;<br>
REVOKE ALL on gs_auditing_policy_access FROM public;<br>
REVOKE ALL on gs_auditing_policy_filters FROM public;<br>
REVOKE ALL on gs_auditing_policy_privileges FROM public;<br>
REVOKE ALL on gs_policy_label FROM public;<br>
REVOKE ALL on gs_masking_policy FROM public;<br>
REVOKE ALL on gs_masking_policy_actions FROM public;<br>
REVOKE ALL on gs_masking_policy_filters FROM public;<br>
GRANT USAGE ON SCHEMA sqladvisor TO PUBLIC;<br>
GRANT USAGE ON SCHEMA dbe_pldebugger TO PUBLIC;<br>
<br>

-- __7. Setup bucketmap len__<br>
-- Note: need to set the value for g_bucket_len, which is 16384 by default.<br>
INSERT INTO gs_global_config VALUES ('buckets_len', 16384);<br>
<br>

-- __8. Setup schema__<br>
-- Run all statements from src/common/backend/catalog/information_schema.sql<br>
<br>
-- Note: need to set the value for infoversion, which consists of major, minor, micro and letterversion.<br>
-- The strange infoversion seems to be "09.08.0007abc".<br>
UPDATE information_schema.sql_implementation_info \\<br>
&nbsp;&nbsp;&nbsp;&nbsp; SET character_value = '09.08.0007abc' \\<br>
&nbsp;&nbsp;&nbsp;&nbsp; WHERE implementation_info_name = 'DBMS VERSION';<br>

-- Note: need to set the value for buf_features, which is sql_features.txt<br>
COPY information_schema.sql_features\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; (feature_id, feature_name, sub_feature_id,\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; sub_feature_name, is_supported, comments)\\<br>
&nbsp;&nbsp;&nbsp;&nbsp; FROM E'sql_features.txt';<br>
<br>

-- __9. Load update__<br>
-- Add new context into update_systbl.sql to update system tables.<br>
-- TODO: Note: need to set the value for share_path, which seems to be src/common/backend/catalog.<br>
COPY pg_cast FROM 'share_path/pg_cast_oid.txt' USING DELIMITERS '|';<br>
<br>

-- __10. Setup snapshots__<br>
-- Need to read an array of snapshot files, and do the setup in each snapshot file.<br>
-- All snapshot files are stored in the snapshot_files array.<br>
-- Names of all the snapshots are stored in the snapshot_names array: <br>
-- {"schema", "create", "prepare", "sample", "publish", "purge"}<br>
<br>

-- __Next steps__<br>
-- make_template0() <br>
-- make_postgres()<br>


