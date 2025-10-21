#!/bin/bash
# Script para actualizar la base de datos SQLite local
# Ejecuta este script cada vez que quieras ver los datos más recientes

echo "Copiando base de datos SQLite actualizada desde el contenedor..."
docker cp airflow-docker-airflow-webserver-1:/opt/airflow/airflow.db ./airflow.db
echo "Base de datos actualizada en: $(pwd)/airflow.db"
echo "Puedes refrescar DBeaver para ver los datos más recientes"