--
-- REINDEX CONCURRENTLY
--
CREATE TABLE concur_reindex_tab (c1 int);
-- REINDEX
REINDEX TABLE concur_reindex_tab; -- notice
REINDEX TABLE CONCURRENTLY concur_reindex_tab; -- notice
ALTER TABLE concur_reindex_tab ADD COLUMN c2 text; -- add toast index
-- Normal index with integer column
CREATE UNIQUE INDEX concur_reindex_ind1 ON concur_reindex_tab(c1);
-- Normal index with text column
CREATE INDEX concur_reindex_ind2 ON concur_reindex_tab(c2);
-- UNION INDEX index with expression
CREATE UNIQUE INDEX concur_reindex_ind3 ON concur_reindex_tab(abs(c1));
-- Duplicates column names error
CREATE INDEX concur_reindex_ind4 ON concur_reindex_tab(c1, c1, c2);
-- Create table for check on foreign key dependence switch with indexes swapped
ALTER TABLE concur_reindex_tab ADD PRIMARY KEY USING INDEX concur_reindex_ind1;
CREATE TABLE concur_reindex_tab2 (c1 int REFERENCES concur_reindex_tab);
INSERT INTO concur_reindex_tab VALUES (1, 'a');
INSERT INTO concur_reindex_tab VALUES (2, 'a');
-- Check materialized views
CREATE MATERIALIZED VIEW concur_reindex_matview AS SELECT * FROM concur_reindex_tab;
REINDEX INDEX CONCURRENTLY concur_reindex_ind1;
REINDEX TABLE CONCURRENTLY concur_reindex_tab;
REINDEX TABLE CONCURRENTLY concur_reindex_matview;
-- Check views 
CREATE VIEW concur_reindex_view AS SELECT * FROM concur_reindex_tab;
REINDEX TABLE CONCURRENTLY concur_reindex_view; -- Error
-- Check that comments are preserved
CREATE TABLE testcomment (i int);
CREATE INDEX testcomment_idx1 ON testcomment(i);
COMMENT ON INDEX testcomment_idx1 IS 'test comment';
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
REINDEX TABLE testcomment;
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
REINDEX TABLE CONCURRENTLY testcomment;
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
DROP TABLE testcomment;

-- Check error
-- Cannot run inside a transaction block
BEGIN;
REINDEX TABLE CONCURRENTLY concur_reindex_tab;
COMMIT;
REINDEX TABLE CONCURRENTLY pg_database; -- no shared relation
REINDEX TABLE CONCURRENTLY pg_class; -- no catalog relations
REINDEX SYSTEM CONCURRENTLY postgres; -- not allowed for SYSTEM

-- Check the relation status, there should not be invalid indexe
\d concur_reindex_tab
DROP VIEW concur_reindex_view;
DROP MATERIALIZED VIEW concur_reindex_matview;
DROP TABLE concur_reindex_tab, concur_reindex_tab2;
