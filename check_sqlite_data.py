#!/usr/bin/env python3
"""
Script para verificar datos en SQLite y potencialmente migrarlos a PostgreSQL
"""

import sqlite3
import json
import os

def check_sqlite_data():
    """Verifica qué datos hay en los archivos SQLite"""
    
    sqlite_files = [
        'airflow_countries_stations.db',
        'airflow_complete.db', 
        'airflow_final.db',
        'airflow.db'
    ]
    
    for db_file in sqlite_files:
        if not os.path.exists(db_file):
            continue
            
        print(f"\n=== Analizando {db_file} ===")
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Obtener lista de tablas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if not tables:
                print("No hay tablas en esta base de datos")
                conn.close()
                continue
            
            print(f"Tablas encontradas: {[table[0] for table in tables]}")
            
            # Verificar datos en tablas relevantes
            relevant_tables = ['openaq_countries', 'openaq_locations', 'openaq_parameters', 'station']
            
            for table_name in relevant_tables:
                if any(table[0] == table_name for table in tables):
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cursor.fetchone()[0]
                    print(f"  - {table_name}: {count} registros")
                    
                    if count > 0:
                        # Mostrar ejemplo de datos
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 2;")
                        sample = cursor.fetchall()
                        print(f"    Ejemplo: {sample[0] if sample else 'Sin datos'}")
            
            conn.close()
            
        except Exception as e:
            print(f"Error analizando {db_file}: {str(e)}")
    
    return sqlite_files

def find_best_sqlite_file():
    """Encuentra el archivo SQLite con más datos completos"""
    best_file = None
    max_records = 0
    
    sqlite_files = [
        'airflow_countries_stations.db',
        'airflow_complete.db', 
        'airflow_final.db'
    ]
    
    for db_file in sqlite_files:
        if not os.path.exists(db_file):
            continue
            
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Contar registros totales en tablas relevantes
            total_records = 0
            relevant_tables = ['openaq_countries', 'openaq_locations', 'openaq_parameters', 'station']
            
            for table_name in relevant_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cursor.fetchone()[0]
                    total_records += count
                except:
                    continue
            
            if total_records > max_records:
                max_records = total_records
                best_file = db_file
                
            conn.close()
            
        except Exception as e:
            continue
    
    return best_file, max_records

if __name__ == "__main__":
    print("Verificando datos en archivos SQLite...")
    check_sqlite_data()
    
    best_file, records = find_best_sqlite_file()
    if best_file:
        print(f"\n🏆 Mejor archivo: {best_file} con {records} registros totales")
    else:
        print("\n⚠️ No se encontraron datos relevantes en SQLite")