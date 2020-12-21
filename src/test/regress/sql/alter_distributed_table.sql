CREATE SCHEMA alter_distributed_table;
SET search_path TO alter_distributed_table;
SET citus.shard_count TO 4;

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a', colocate_with := 'none');
INSERT INTO dist_table VALUES (1, 1), (2, 2), (3, 3);

CREATE TABLE colocation_table (a INT, b INT);
SELECT create_distributed_table ('colocation_table', 'a', colocate_with := 'none');

CREATE TABLE colocation_table_2 (a INT, b INT);
SELECT create_distributed_table ('colocation_table_2', 'a', colocate_with := 'none');


SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;

-- test altering distribution column
SELECT alter_distributed_table('dist_table', distribution_column := 'b');
SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;

-- test altering shard count
SELECT alter_distributed_table('dist_table', shard_count := 6);
SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;

-- test altering colocation, note that shard count will also change
SELECT alter_distributed_table('dist_table', colocate_with := 'alter_distributed_table.colocation_table');
SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;

-- test altering shard count with cascading, note that the colocation will be kept
SELECT alter_distributed_table('dist_table', shard_count := 8, cascade_to_colocated := true);
SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;

-- test altering shard count without cascading, note that the colocation will be broken
SELECT alter_distributed_table('dist_table', shard_count := 10, cascade_to_colocated := false);
SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE "Name" IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY "Colocation ID" ORDER BY 1;


-- test error messages
-- test nothing to change
SELECT alter_distributed_table('dist_table');
SELECT alter_distributed_table('dist_table', cascade_to_colocated := false);

-- no operation UDF calls
SELECT alter_distributed_table('dist_table', distribution_column := 'b');
SELECT alter_distributed_table('dist_table', shard_count := 10);
-- first colocate the tables, then try to re-colococate
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table');
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table');

-- test cascading distribution column, should error
SELECT alter_distributed_table('dist_table', distribution_column := 'a', cascade_to_colocated := true);
SELECT alter_distributed_table('dist_table', distribution_column := 'a', shard_count:=12, colocate_with:='colocation_table_2', cascade_to_colocated := true);

-- test nothing to cascade
SELECT alter_distributed_table('dist_table', cascade_to_colocated := true);

-- test cascading colocate_with := 'none'
SELECT alter_distributed_table('dist_table', colocate_with := 'none', cascade_to_colocated := true);

-- test changing shard count of a colocated table without cascade_to_colocated, should error
SELECT alter_distributed_table('dist_table', shard_count := 14);

-- test changing shard count of a non-colocated table without cascade_to_colocated, shouldn't error
SELECT alter_distributed_table('dist_table', colocate_with := 'none');
SELECT alter_distributed_table('dist_table', shard_count := 14);

-- test altering a table into colocating with a table but giving a different shard count
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table', shard_count := 16);

-- test colocation with distribution columns with different data types
CREATE TABLE different_type_table (a TEXT);
SELECT create_distributed_table('different_type_table', 'a');

SELECT alter_distributed_table('dist_table', colocate_with := 'different_type_table');
SELECT alter_distributed_table('dist_table', distribution_column := 'a', colocate_with := 'different_type_table');

-- test shard_count := 0
SELECT alter_distributed_table('dist_table', shard_count := 0);

DROP SCHEMA alter_distributed_table CASCADE;
