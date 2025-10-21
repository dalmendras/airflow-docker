#!/usr/bin/env python3
"""
Script para verificar que se cargaron todos los países en PostgreSQL
"""

import psycopg2
import json

def get_postgres_connection():
    """
    Establece conexión con PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='openaq_data',
            user='openaq_user',
            password='openaq_password'
        )
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {str(e)}")
        raise

def verify_countries_data():
    """Verifica los datos de países en PostgreSQL"""
    print("=== Verificando países en PostgreSQL ===")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Contar total de países
    cursor.execute("SELECT COUNT(*) FROM openaq_countries;")
    total_countries = cursor.fetchone()[0]
    print(f"Total de países en la base de datos: {total_countries}")
    
    # Mostrar algunos países de ejemplo
    cursor.execute("""
        SELECT country_id, code, name, datetimeFirst, datetimeLast 
        FROM openaq_countries 
        ORDER BY name 
        LIMIT 10;
    """)
    
    sample_countries = cursor.fetchall()
    print(f"\nPrimeros 10 países (ordenados alfabéticamente):")
    for country in sample_countries:
        print(f"  ID: {country[0]}, Código: {country[1]}, Nombre: {country[2]}")
        print(f"    Primer dato: {country[3]}, Último dato: {country[4]}")
    
    # Verificar distribución por código de país
    cursor.execute("""
        SELECT code, COUNT(*) as count 
        FROM openaq_countries 
        GROUP BY code 
        HAVING COUNT(*) > 1
        ORDER BY count DESC;
    """)
    
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"\n⚠️  Códigos de país duplicados:")
        for dup in duplicates:
            print(f"  {dup[0]}: {dup[1]} registros")
    else:
        print(f"\n✅ No hay códigos de país duplicados")
    
    # Verificar países con más datos
    cursor.execute("""
        SELECT code, name, 
               EXTRACT(YEAR FROM datetimeFirst) as first_year,
               EXTRACT(YEAR FROM datetimeLast) as last_year,
               (datetimeLast - datetimeFirst) as duration
        FROM openaq_countries 
        WHERE datetimeFirst IS NOT NULL AND datetimeLast IS NOT NULL
        ORDER BY (datetimeLast - datetimeFirst) DESC
        LIMIT 5;
    """)
    
    longest_data = cursor.fetchall()
    print(f"\nPaíses con mayor duración de datos:")
    for country in longest_data:
        print(f"  {country[1]} ({country[0]}): {country[2]:.0f}-{country[3]:.0f}")
    
    # Verificar parámetros por país
    cursor.execute("""
        SELECT code, name, jsonb_array_length(parameters) as param_count
        FROM openaq_countries 
        WHERE parameters IS NOT NULL
        ORDER BY jsonb_array_length(parameters) DESC
        LIMIT 5;
    """)
    
    param_rich_countries = cursor.fetchall()
    print(f"\nPaíses con más parámetros disponibles:")
    for country in param_rich_countries:
        print(f"  {country[1]} ({country[0]}): {country[2]} parámetros")
    
    conn.close()
    
    return total_countries

if __name__ == "__main__":
    try:
        total = verify_countries_data()
        
        print(f"\n=== RESUMEN ===")
        if total == 140:
            print(f"✅ Éxito: Se cargaron correctamente los {total} países esperados")
        elif total > 100:
            print(f"✅ Mejora: Se cargaron {total} países (más que los 100 anteriores)")
        else:
            print(f"⚠️  Problema: Solo se cargaron {total} países (se esperaban 140)")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")