-- citus--10.0-1--9.5-1
-- this is an empty downgrade path since citus--9.5-1--10.0-1.sql is empty for now

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP VIEW public.citus_tables;
DROP FUNCTION pg_catalog.citus_total_relation_size(regclass,boolean);
DROP FUNCTION pg_catalog.citus_set_coordinator_host(text,int,noderole,name);

#include "../udfs/citus_total_relation_size/7.0-1.sql"
