#!/bin/sh
echo "Perform Vespa benchmark test (timexxxxxx secs)..."
container=containerxxxxxx

#-i meaning ignore the first 20 results; for system warming up
#query format is vespa get option. See: https://docs.vespa.ai/en/performance/vespa-benchmarking.html
/usr/local/bin/docker cp query_filexxxxxx $container:/tmp/query.txt

/usr/local/bin/docker exec $container bash -c "/opt/vespa/bin/vespa-fbench -n clientxxxxxx -c 0 -i 20 -s timexxxxxx -q /tmp/query.txt -o /tmp/vespa_performance.txt localhost 8080" > report_filexxxxxx