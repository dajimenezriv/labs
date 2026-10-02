-- What you'd run against Azure Flexible Server as the admin (azure_pg_admin, not a superuser).
-- In Azure, pg_stat_statements also has to be allow-listed in the azure.extensions server parameter.
CREATE EXTENSION pg_stat_statements;

-- The exporter gets its own login with read-only access to the stats views, never the admin.
CREATE ROLE monitoring LOGIN PASSWORD 'monitoring';
GRANT pg_monitor TO monitoring;
