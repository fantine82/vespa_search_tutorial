#!/bin/sh

echo "Create docker virtual network..."
docker network create tutorial-search
echo "done"

echo "Launch docker containers for jupyter notebook and vespa..."
docker-compose -f ./docker-compose-notebook.yaml up -d
docker-compose -f ./docker-compose-vespa.yaml up -d
echo "done"

echo "Sleep for 10 secs..."
sleep 10

echo "Copy deployment files to vespa container..."
docker cp ../resources/application tutorialvespa:/tmp/application

echo "Deploy vespa configurations..."
docker exec tutorialvespa bash -c "/opt/vespa/bin/vespa deploy /tmp/application/"
docker exec tutorialvespa bash -c "ls -l /opt/vespa/var/db/vespa/config_server/serverdb/tenants/default/sessions/"

echo "done. Check session availability..."

echo "Find ip address for vespa..."
docker inspect tutorialvespa | grep IPAddress

echo "Please replace 'vespa_url_local' in /system/src/constant.py from 'http://localhost:8080' to 'http://{this_vespa_ip_address}:8080'"



