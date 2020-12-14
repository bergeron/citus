SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven
\gset
\if :server_version_above_eleven
\else
\q
\endif


CREATE SCHEMA alter_table_set_access_method;
SET search_path TO alter_table_set_access_method;
SET citus.shard_count TO 4;

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a');
INSERT INTO dist_table VALUES (1, 1), (2, 2), (3, 3);

SELECT "Name", "Access Method" FROM public.citus_tables WHERE "Name"::text = 'dist_table' ORDER BY 1;
SELECT alter_table_set_access_method('dist_table', 'columnar');
SELECT "Name", "Access Method" FROM public.citus_tables WHERE "Name"::text = 'dist_table' ORDER BY 1;


-- test partitions
CREATE TABLE partitioned_table (id INT, a INT) PARTITION BY RANGE (id);
CREATE TABLE partitioned_table_1_5 PARTITION OF partitioned_table FOR VALUES FROM (1) TO (5);
CREATE TABLE partitioned_table_6_10 PARTITION OF partitioned_table FOR VALUES FROM (6) TO (10);
SELECT create_distributed_table('partitioned_table', 'id');
INSERT INTO partitioned_table VALUES (2, 12), (7, 2);

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT "Name"::text, "Access Method" FROM public.citus_tables WHERE "Name"::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

-- test altering the partition's access method
SELECT alter_table_set_access_method('partitioned_table_1_5', 'columnar');

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT "Name"::text, "Access Method" FROM public.citus_tables WHERE "Name"::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

DROP SCHEMA alter_table_set_access_method CASCADE;
