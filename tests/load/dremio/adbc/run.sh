#!/usr/bin/env bash

set -euxo pipefail

DREMIO_URL=http://dremio:9047

# Poll the Dremio "live" endpoint until ready
until (curl "${DREMIO_URL}/live") do echo '...waiting...' && sleep 1; done;

python adbc.py