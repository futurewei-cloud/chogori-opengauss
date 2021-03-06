--GUC parameters pre check
set IsInplaceUpgrade=on;

select set_working_grand_version_num_manually(82000);

--install table, view, function for cascade drop test
create table inplace_upgrade_cas (a int, b int);
create view inplace_upgrade_cas_v1 as select * from inplace_upgrade_cas;
create or replace function inplace_upgrade_cas_f1() returns setof inplace_upgrade_cas
as $$
declare
r inplace_upgrade_cas%rowtype;
begin
for r in select * from inplace_upgrade_cas
loop
return next r;
end loop;
return;
end; $$
language 'plpgsql';

--install upgrade support functions and turn on inplace upgrade by setting an older working grand version
create or replace function start_inplace_upgrade_functions_manually() returns void as '$libdir/pg_upgrade_support' language c strict not fenced;
create or replace function stop_inplace_upgrade_functions_manually() returns void as '$libdir/pg_upgrade_support' language c strict not fenced;

create or replace function change_working_grand_version()
returns void
as $$
declare
row_name record;
query_str text;
query_str_nodes text;
begin
query_str_nodes := 'select node_name from pgxc_node';
for row_name in execute(query_str_nodes) loop
query_str := 'execute direct on (' || row_name.node_name || ') ''select start_inplace_upgrade_functions_manually()''';
execute(query_str);
query_str := 'execute direct on (' || row_name.node_name || ') ''select set_working_grand_version_num_manually(82000)''';
execute(query_str);
query_str := 'execute direct on (' || row_name.node_name || ') ''select stop_inplace_upgrade_functions_manually()''';
execute(query_str);
end loop;
return;
end; $$
language 'plpgsql';

select change_working_grand_version();
