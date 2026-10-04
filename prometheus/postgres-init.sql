-- What you'd run against Azure Flexible Server as the admin (azure_pg_admin, not a superuser).
-- In Azure, pg_stat_statements also has to be allow-listed in the azure.extensions server parameter.

-- The exporter gets its own login with read-only access to the stats views, never the admin.
CREATE ROLE monitoring LOGIN PASSWORD 'monitoring';
GRANT pg_monitor TO monitoring;

-- The app owns its database; the default "postgres" database stays empty.
CREATE ROLE users LOGIN PASSWORD 'users';
CREATE DATABASE users OWNER users;

\connect users
-- The stats are server-wide, but the view only exists in databases where the extension is
-- created, so it goes where the exporter connects.
CREATE EXTENSION pg_stat_statements;
