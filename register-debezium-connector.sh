#!/bin/bash

echo "Waiting for Debezium to be ready..."
sleep 20

echo "Registering PostgreSQL CDC connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d '{
  "name": "crm-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "wtc_prod",
    "database.server.name": "crm_server",
    "table.include.list": "crm_system.accounts,crm_system.addresses,crm_system.devices",
    "plugin.name": "pgoutput",
    "topic.prefix": "crm",
    "slot.name": "debezium_crm",
    "publication.name": "debezium_publication"
  }
}'

echo ""
echo " Connector registered!"
echo "Check status: curl http://localhost:8083/connectors/crm-postgres-connector/status"
