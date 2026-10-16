#!/bin/bash
# keep the database time zone away from UTC, a UTC database hides every conversion bug. the change
# needs a restart and fails once a LOCAL TIME ZONE column holds data, so only apply it once
CURRENT=$(sqlplus -S / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT DBTIMEZONE FROM DUAL;
EXIT;
SQL
)
if [[ "$(echo "$CURRENT" | tr -d '[:space:]')" == "+05:00" ]]; then
    echo "DBTIMEZONE is already +05:00"
    exit 0
fi
sqlplus -S / as sysdba <<'SQL'
ALTER DATABASE SET TIME_ZONE = '+05:00';
SHUTDOWN IMMEDIATE;
STARTUP;
ALTER PLUGGABLE DATABASE ALL OPEN;
SELECT 'DBTIMEZONE is now ' || DBTIMEZONE FROM DUAL;
EXIT;
SQL
