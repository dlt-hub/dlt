CREATE DATABASE $database;
REVOKE ALL PRIVILEGES ON DATABASE $database FROM PUBLIC;

-- \connect $database
DROP SCHEMA public;
-- SET search_path = data; don't use search paths
CREATE USER $user WITH PASSWORD '$password';
GRANT CONNECT ON DATABASE $database TO $user;
ALTER DATABASE $database OWNER TO $user


-- minimum permissions to all schemas created by the user: so we can have db owner vs. schema owner
ALTER SCHEMA data OWNER TO $user

GRANT CREATE ON SCHEMA data TO $user

ALTER DEFAULT PRIVILEGES FOR ROLE ${database}_owner GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO $user
ALTER DEFAULT PRIVILEGES FOR ROLE ${database}_owner GRANT SELECT, UPDATE ON SEQUENCES TO $user
ALTER DEFAULT PRIVILEGES FOR ROLE ${database}_owner GRANT EXECUTE ON FUNCTIONS TO $user