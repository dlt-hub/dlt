-- When creating the user, a dedicated tablespace is preferrable. We have to create them in all containers/databases,
-- in case of 23-free-lite docker image it's CDB as well as the PDB (by default, FREEPDB1)
-- User name for common users has to start from C##

-- Creating the tablespace in both databases. Startup script is executed as SYSDBA, so by default container = CDB$ROOT
CREATE TABLESPACE dlt_ts
  DATAFILE '/opt/oracle/oradata/FREE/dlt_ts01.dbf'
    SIZE 5G
    AUTOEXTEND ON NEXT 512M MAXSIZE 10G
  SEGMENT SPACE MANAGEMENT AUTO
  ONLINE;

ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE TABLESPACE dlt_ts
  DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/dlt_ts01.dbf'
    SIZE 5G
    AUTOEXTEND ON NEXT 512M MAXSIZE 10G
  SEGMENT SPACE MANAGEMENT AUTO
  ONLINE;

ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Creating the user
CREATE USER C##LOADER IDENTIFIED BY "loader"
  DEFAULT TABLESPACE dlt_ts
  TEMPORARY TABLESPACE temp
  QUOTA UNLIMITED ON dlt_ts;

-- Granting the necessary permissions to the user
GRANT
  CREATE SESSION,
  CREATE TABLE,
  CREATE VIEW,
  CREATE SEQUENCE,
  CREATE PROCEDURE
TO C##LOADER;
