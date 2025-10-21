#!/usr/bin/env python3
"""
Script para migrar datos de SQLite a PostgreSQL
"""

import sqlite3
import psycopg2
import psycopg2.extras
import json
import os

def get_postgres_connection():
    """Obtiene conexión a PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            port=os.environ.get('POSTGRES_PORT', '5432'),
            database=os.environ.get('POSTGRES_DB', 'openaq_data'),
            user=os.environ.get('POSTGRES_USER', 'openaq_user'),
            password=os.environ.get('POSTGRES_PASSWORD', 'openaq_password')
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {str(e)}")
        raise

def create_postgres_tables():
    """Crea las tablas en PostgreSQL si no existen"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Tabla de países
        countries_table_sql = """
        CREATE TABLE IF NOT EXISTS openaq_countries (
            id SERIAL PRIMARY KEY,
            country_id INTEGER UNIQUE,
            code VARCHAR(10),
            name VARCHAR(255),
            datetimeFirst TIMESTAMP,
            datetimeLast TIMESTAMP,
            parameters JSONB,
            locations INTEGER DEFAULT 0,
            sources INTEGER DEFAULT 0,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Tabla de estaciones (compatible con el modelo existente)
        stations_table_sql = """
        CREATE TABLE IF NOT EXISTS station (
            id INTEGER NOT NULL,
            name VARCHAR(255),
            locality VARCHAR(255),
            country_id INTEGER,
            provider_id INTEGER,
            provider_name VARCHAR(255),
            sensor_id INTEGER NOT NULL,
            sensor_name VARCHAR(255),
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            timezone VARCHAR(100),
            is_mobile BOOLEAN,
            is_monitor BOOLEAN,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, sensor_id),
            FOREIGN KEY (country_id) REFERENCES openaq_countries(country_id)
        );
        """
        
        # Índices
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_countries_code ON openaq_countries(code);",
            "CREATE INDEX IF NOT EXISTS idx_countries_country_id ON openaq_countries(country_id);",
            "CREATE INDEX IF NOT EXISTS idx_station_country ON station(country_id);",
            "CREATE INDEX IF NOT EXISTS idx_station_provider ON station(provider_id);",
            "CREATE INDEX IF NOT EXISTS idx_station_location ON station(latitude, longitude);"
        ]
        
        print("Creando tablas en PostgreSQL...")
        cursor.execute(countries_table_sql)
        cursor.execute(stations_table_sql)
        
        for index_sql in indexes_sql:
            cursor.execute(index_sql)
        
        conn.commit()
        print("✅ Tablas creadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()

def migrate_countries_data():
    """Migra datos de países de SQLite a PostgreSQL"""
    sqlite_file = 'airflow_countries_stations.db'
    
    if not os.path.exists(sqlite_file):
        print(f"❌ Archivo {sqlite_file} no encontrado")
        return
    
    # Conectar a SQLite
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()
    
    # Conectar a PostgreSQL
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Leer datos de SQLite
        sqlite_cursor.execute("SELECT * FROM openaq_countries;")
        countries = sqlite_cursor.fetchall()
        
        print(f"Migrando {len(countries)} países...")
        
        # Limpiar tabla PostgreSQL
        pg_cursor.execute("DELETE FROM openaq_countries;")
        
        # Migrar datos
        upsert_sql = """
        INSERT INTO openaq_countries 
        (country_id, code, name, datetimeFirst, datetimeLast, parameters, locations, sources, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_id) 
        DO UPDATE SET 
            code = EXCLUDED.code,
            name = EXCLUDED.name,
            datetimeFirst = EXCLUDED.datetimeFirst,
            datetimeLast = EXCLUDED.datetimeLast,
            parameters = EXCLUDED.parameters,
            locations = EXCLUDED.locations,
            sources = EXCLUDED.sources,
            extracted_at = EXCLUDED.extracted_at;
        """
        
        for country in countries:
            # country = (id, country_id, code, name, datetimeFirst, datetimeLast, parameters, locations, sources, extracted_at)
            pg_cursor.execute(upsert_sql, (
                country[1],  # country_id
                country[2],  # code
                country[3],  # name
                country[4],  # datetimeFirst
                country[5],  # datetimeLast
                country[6],  # parameters (ya es JSON string)
                country[7] if len(country) > 7 else 0,  # locations
                country[8] if len(country) > 8 else 0,  # sources
                country[9] if len(country) > 9 else None  # extracted_at
            ))
        
        pg_conn.commit()
        
        # Verificar migración
        pg_cursor.execute("SELECT COUNT(*) FROM openaq_countries;")
        count = pg_cursor.fetchone()[0]
        print(f"✅ Migrados {count} países exitosamente")
        
    except Exception as e:
        print(f"❌ Error migrando países: {str(e)}")
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

def migrate_stations_data():
    """Migra datos de estaciones de SQLite a PostgreSQL"""
    sqlite_file = 'airflow_countries_stations.db'
    
    if not os.path.exists(sqlite_file):
        print(f"❌ Archivo {sqlite_file} no encontrado")
        return
    
    # Conectar a SQLite
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()
    
    # Conectar a PostgreSQL
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Leer datos de SQLite
        sqlite_cursor.execute("SELECT * FROM station;")
        stations = sqlite_cursor.fetchall()
        
        print(f"Migrando {len(stations)} registros de estaciones...")
        
        # Limpiar tabla PostgreSQL
        pg_cursor.execute("DELETE FROM station;")
        
        # Migrar datos
        upsert_sql = """
        INSERT INTO station 
        (id, name, locality, country_id, provider_id, provider_name, sensor_id, sensor_name, 
         latitude, longitude, timezone, is_mobile, is_monitor, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, sensor_id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            locality = EXCLUDED.locality,
            country_id = EXCLUDED.country_id,
            provider_id = EXCLUDED.provider_id,
            provider_name = EXCLUDED.provider_name,
            sensor_name = EXCLUDED.sensor_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone,
            is_mobile = EXCLUDED.is_mobile,
            is_monitor = EXCLUDED.is_monitor,
            extracted_at = EXCLUDED.extracted_at;
        """
        
        for station in stations:
            # station = (id, name, locality, country_id, provider_id, provider_name, sensor_id, sensor_name, lat, lon, timezone, is_mobile, is_monitor, extracted_at)
            pg_cursor.execute(upsert_sql, station)
        
        pg_conn.commit()
        
        # Verificar migración
        pg_cursor.execute("SELECT COUNT(*) FROM station;")
        count = pg_cursor.fetchone()[0]
        print(f"✅ Migrados {count} registros de estaciones exitosamente")
        
    except Exception as e:
        print(f"❌ Error migrando estaciones: {str(e)}")
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

def verify_migration():
    """Verifica que la migración fue exitosa"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar países
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT code) FROM openaq_countries;")
        countries_stats = cursor.fetchone()
        
        # Verificar estaciones
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM station;")
        stations_stats = cursor.fetchone()
        
        print("\n=== VERIFICACIÓN DE MIGRACIÓN ===")
        print(f"Países: {countries_stats[0]} registros, {countries_stats[1]} códigos únicos")
        print(f"Estaciones: {stations_stats[0]} registros, {stations_stats[1]} estaciones únicas")
        
        # Ejemplo de datos
        cursor.execute("SELECT code, name FROM openaq_countries WHERE code = 'CL' LIMIT 1;")
        chile = cursor.fetchone()
        if chile:
            print(f"Ejemplo - Chile: {chile}")
        
        cursor.execute("SELECT COUNT(*) FROM station WHERE country_id = (SELECT country_id FROM openaq_countries WHERE code = 'CL' LIMIT 1);")
        chile_stations = cursor.fetchone()[0]
        print(f"Estaciones de Chile: {chile_stations}")
        
    except Exception as e:
        print(f"❌ Error en verificación: {str(e)}")
    finally:
        conn.close()

def main():
    """Función principal de migración"""
    print("=== MIGRACIÓN DE SQLITE A POSTGRESQL ===")
    
    try:
        # Paso 1: Crear tablas
        create_postgres_tables()
        
        # Paso 2: Migrar países
        migrate_countries_data()
        
        # Paso 3: Migrar estaciones
        migrate_stations_data()
        
        # Paso 4: Verificar migración
        verify_migration()
        
        print("\n🎉 ¡Migración completada exitosamente!")
        
    except Exception as e:
        print(f"\n❌ Error en la migración: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)